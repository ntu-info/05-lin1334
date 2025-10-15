# app.py
from flask import Flask, jsonify, abort, send_file, request
import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

_engine = None


def get_engine():
    """
    Initializes and returns the SQLAlchemy engine, caching it for reuse.
    """
    global _engine
    if _engine is not None:
        return _engine

    db_url = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Missing DB_URL or DATABASE_URL environment variable.")

    # Normalize old 'postgres://' scheme to 'postgresql://'
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]

    _engine = create_engine(
        db_url,
        pool_pre_ping=True,
    )
    return _engine


def parse_coords(coords_str: str):
    """
    Parses a coordinate string like 'x_y_z' into a tuple of floats.
    Returns (x, y, z) or None if invalid.
    """
    try:
        parts = [float(p) for p in coords_str.split('_')]
        if len(parts) == 3:
            return tuple(parts)
    except (ValueError, TypeError):
        pass
    return None


def create_app():
    app = Flask(__name__)

    @app.get("/", endpoint="health")
    def health():
        return "<p>Server working!</p>"

    @app.get("/img", endpoint="show_img")
    def show_img():
        return send_file("amygdala.gif", mimetype="image/gif")

    @app.get("/dissociate/terms/<term_a>/<term_b>", endpoint="dissociate_terms")
    def dissociate_by_term(term_a, term_b):
        """
        Returns studies associated with term_a but NOT term_b.
        """
        sql = """
            SELECT study_id FROM ns.annotations_terms WHERE term = :term_a
            EXCEPT
            SELECT study_id FROM ns.annotations_terms WHERE term = :term_b
            LIMIT 250;
        """
        try:
            engine = get_engine()
            with engine.connect() as conn:
                result = conn.execute(text(sql), {"term_a": term_a, "term_b": term_b})
                study_ids = [row[0] for row in result]
            return jsonify({
                "term_a": term_a,
                "term_b": term_b,
                "study_ids": study_ids,
            }), 200
        except Exception as e:
            return jsonify({"error": f"Database error: {str(e)}"}), 500

    @app.get("/dissociate/locations/<coords_a>/<coords_b>", endpoint="dissociate_locations")
    def dissociate_by_location(coords_a, coords_b):
        """
        Returns studies with activations near coords_a but NOT near coords_b.
        Optional query parameters:
            - radius (float, default=10): search radius in mm
            - bidirectional (bool, default=false): return both A–B and B–A
        """
        radius = request.args.get("radius", default=10, type=float)
        bidirectional = request.args.get("bidirectional", default="false").lower() == "true"

        p_a = parse_coords(coords_a)
        p_b = parse_coords(coords_b)

        if p_a is None or p_b is None:
            abort(400, description="Invalid coordinate format. Use x_y_z (e.g., '0_-52_26').")

        sql = """
            SELECT study_id FROM ns.coordinates
            WHERE ST_DWithin(
                geom,
                ST_SetSRID(ST_MakePoint(:x1, :y1, :z1), 4326),
                :radius
            )
            EXCEPT
            SELECT study_id FROM ns.coordinates
            WHERE ST_DWithin(
                geom,
                ST_SetSRID(ST_MakePoint(:x2, :y2, :z2), 4326),
                :radius
            )
            LIMIT 250;
        """

        try:
            engine = get_engine()
            with engine.connect() as conn:
                # Direction A → B
                res_ab = conn.execute(
                    text(sql),
                    {
                        "x1": p_a[0], "y1": p_a[1], "z1": p_a[2],
                        "x2": p_b[0], "y2": p_b[1], "z2": p_b[2],
                        "radius": radius
                    }
                )
                study_ids_ab = [row[0] for row in res_ab]

                payload = {
                    "coords_a": coords_a,
                    "coords_b": coords_b,
                    "radius": radius,
                    "direction_A_minus_B": {
                        "from": coords_a,
                        "not": coords_b,
                        "count": len(study_ids_ab),
                        "study_ids": study_ids_ab
                    }
                }

                # If bidirectional requested → compute B → A as well
                if bidirectional:
                    res_ba = conn.execute(
                        text(sql),
                        {
                            "x1": p_b[0], "y1": p_b[1], "z1": p_b[2],
                            "x2": p_a[0], "y2": p_a[1], "z2": p_a[2],
                            "radius": radius
                        }
                    )
                    study_ids_ba = [row[0] for row in res_ba]

                    payload["direction_B_minus_A"] = {
                        "from": coords_b,
                        "not": coords_a,
                        "count": len(study_ids_ba),
                        "study_ids": study_ids_ba
                    }

            return jsonify(payload), 200

        except OperationalError as e:
            return jsonify({"error": f"Database connection failed: {str(e)}"}), 500
        except Exception as e:
            return jsonify({"error": f"Query failed: {str(e)}"}), 500

    @app.get("/test_db", endpoint="test_db")
    def test_db():
        """
        Provides a database health check and returns basic counts and samples.
        """
        payload = {"ok": False}
        try:
            engine = get_engine()
            with engine.connect() as conn:
                payload["coordinates_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.coordinates")).scalar()
                payload["metadata_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.metadata")).scalar()
                payload["annotations_terms_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.annotations_terms")).scalar()

                rows = conn.execute(text(
                    "SELECT study_id, ST_X(geom) AS x, ST_Y(geom) AS y, ST_Z(geom) AS z FROM ns.coordinates LIMIT 3"
                )).mappings().all()
                payload["coordinates_sample"] = [dict(r) for r in rows]

                rows = conn.execute(text(
                    "SELECT study_id, title, year FROM ns.metadata LIMIT 3"
                )).mappings().all()
                payload["metadata_sample"] = [dict(r) for r in rows]

                rows = conn.execute(text(
                    "SELECT study_id, contrast_id, term, weight FROM ns.annotations_terms LIMIT 3"
                )).mappings().all()
                payload["annotations_terms_sample"] = [dict(r) for r in rows]

            payload["ok"] = True
            return jsonify(payload), 200

        except Exception as e:
            payload["error"] = str(e)
            return jsonify(payload), 500

    return app


# WSGI entry point
app = create_app()
