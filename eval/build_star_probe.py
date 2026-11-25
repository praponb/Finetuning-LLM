"""Loop probe eval/star_probe.jsonl (11-25, q9df fix round): 30 hand-written `SELECT *` / `SELECT t.*` queries with
IN / NOT IN / EXISTS / NOT EXISTS / scalar subqueries on the 5 hold-out tables of schemas.py (never in training).
q9d / q9df looped to the token limit on this shape (11-24 app test); v18a / q9e / q9de did not. Used to check the fix
(loops + correctness), never trained on, not part of the promotion rule.

Checks (stops on failure): every query runs in the verify_pairs.Checker Spark session and returns >= 1 row; no query
equals an eval / dev query or appears inside a train pair (data_q9e, v18a mix).
  python eval/build_star_probe.py        (repo root, with sql2pyspark-mlx/.venv/bin/python)
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MLX = ROOT / "sql2pyspark-mlx"
sys.path.insert(0, str(MLX))
sys.path.insert(0, str(ROOT / "eval"))
norm = lambda s: " ".join(s.split()).lower().rstrip(";").strip()

S, M = "simple", "medium"
Q: list[tuple[str, str, list[str]]] = [
    # IN / NOT IN, no aliases
    (S, "SELECT * FROM bookings WHERE flight_id IN (SELECT id FROM flights WHERE airline = 'ANA');", []),
    (S, "SELECT * FROM doctors WHERE id NOT IN (SELECT doctor_id FROM appointments WHERE appt_type = 'surgery');", []),
    (S, "SELECT * FROM patients WHERE id IN (SELECT patient_id FROM appointments WHERE status = 'NO_SHOW');", []),
    (S, "SELECT * FROM flights WHERE id NOT IN (SELECT flight_id FROM bookings WHERE seat_class = 'FIRST');", []),
    (S, "SELECT * FROM appointments WHERE doctor_id IN (SELECT id FROM doctors WHERE hospital = 'Bumrungrad') ORDER BY id;", ["id"]),
    (S, "SELECT * FROM patients WHERE id NOT IN (SELECT patient_id FROM appointments WHERE patient_id IS NOT NULL);", []),
    (S, "SELECT * FROM appointments WHERE patient_id IN (SELECT id FROM patients WHERE is_smoker = TRUE) AND fee > 500;", []),
    (S, "select * from bookings where flight_id not in (select id from flights where origin = 'BKK')", []),
    # aliased IN / NOT IN, SELECT t.*
    (M, "SELECT b.* FROM bookings b WHERE b.flight_id IN (SELECT f.id FROM flights f WHERE f.distance_km > 8000);", []),
    (M, "SELECT d.* FROM doctors d WHERE d.id NOT IN (SELECT a.doctor_id FROM appointments a WHERE a.status = 'CANCELLED');", []),
    (M, "SELECT * FROM appointments a WHERE a.patient_id IN (SELECT p.id FROM patients p WHERE p.insurance_type = 'NONE') ORDER BY a.fee DESC, a.id LIMIT 5;", ["fee", "id"]),
    (M, "SELECT p.* FROM patients p WHERE p.id IN (SELECT a.patient_id FROM appointments a WHERE a.appt_type = 'emergency') AND p.gender <> 'X';", []),
    # EXISTS / NOT EXISTS
    (M, "SELECT * FROM doctors d WHERE EXISTS (SELECT 1 FROM appointments a WHERE a.doctor_id = d.id AND a.fee > 2500);", []),
    (M, "SELECT * FROM flights f WHERE NOT EXISTS (SELECT 1 FROM bookings b WHERE b.flight_id = f.id AND b.is_cancelled = TRUE);", []),
    (M, "SELECT * FROM patients p WHERE EXISTS (SELECT 1 FROM appointments a WHERE a.patient_id = p.id);", []),
    (M, "SELECT * FROM patients p WHERE NOT EXISTS (SELECT 1 FROM appointments a WHERE a.patient_id = p.id);", []),
    (M, "SELECT f.* FROM flights f WHERE EXISTS (SELECT 1 FROM bookings b WHERE b.flight_id = f.id AND b.seat_class = 'BUSINESS') ORDER BY f.id;", ["id"]),
    (M, "SELECT a.* FROM appointments a WHERE NOT EXISTS (SELECT 1 FROM doctors d WHERE d.id = a.doctor_id AND d.specialty = 'General');", []),
    (M, "SELECT * FROM bookings b WHERE EXISTS (SELECT * FROM flights f WHERE f.id = b.flight_id AND f.destination = 'LHR');", []),
    (M, "SELECT * FROM doctors d WHERE NOT EXISTS (SELECT 1 FROM appointments a WHERE a.doctor_id = d.id AND a.status = 'NO_SHOW') ORDER BY d.years_experience DESC, d.id;", ["years_experience", "id"]),
    (M, "select * from appointments a where exists (select 1 from patients p where p.id = a.patient_id and p.city is null)", []),
    # combined with other filters / scalar subqueries
    (M, "SELECT * FROM bookings WHERE price > (SELECT AVG(price) FROM bookings) AND flight_id IN (SELECT id FROM flights WHERE airline = 'Delta');", []),
    (M, "SELECT * FROM appointments WHERE fee > (SELECT AVG(fee) FROM appointments WHERE appt_type = 'checkup');", []),
    (M, "SELECT * FROM flights WHERE base_fare < (SELECT MAX(base_fare) FROM flights) / 4 AND id NOT IN (SELECT flight_id FROM bookings WHERE loyalty_tier = 'GOLD');", []),
    (M, "SELECT * FROM patients WHERE city IN ('Bangkok', 'Tokyo') AND id NOT IN (SELECT patient_id FROM appointments WHERE patient_id IS NOT NULL AND fee > 1000);", []),
    (M, "SELECT * FROM doctors WHERE specialty IN (SELECT specialty FROM doctors WHERE years_experience > 30);", []),
    (M, "SELECT * FROM bookings b WHERE b.price > 3000 AND NOT EXISTS (SELECT 1 FROM flights f WHERE f.id = b.flight_id AND f.origin = f.destination);", []),
    (M, "SELECT * FROM appointments a WHERE EXISTS (SELECT 1 FROM doctors d WHERE d.id = a.doctor_id AND d.hospital = 'St. Mary') AND a.status <> 'CANCELLED' ORDER BY a.appt_date, a.id;", ["appt_date", "id"]),
    (M, "SELECT DISTINCT * FROM flights WHERE id IN (SELECT flight_id FROM bookings WHERE price > 8000);", []),
    (M, "SELECT * FROM patients WHERE id IN (SELECT patient_id FROM appointments WHERE doctor_id IN (SELECT id FROM doctors WHERE specialty = 'Pediatrics'));", []),
]


def main() -> None:
    from build_dev_short import eval_sql
    assert len(Q) == 30, len(Q)
    sqls = [norm(s) for _, s, _ in Q]
    assert len(set(sqls)) == 30, "duplicate probe queries"
    evs = eval_sql() | {norm(json.loads(x)["sql"]) for x in (ROOT / "eval/dev_short.jsonl").read_text().splitlines()}
    same_as_eval = [s for s in sqls if s in evs]
    train = []
    for f in (MLX / "qwen35_pilot/data_q9e/train.jsonl", MLX / "complex_v17/data_mix/train.jsonl"):
        train += [norm(json.loads(x)["messages"][1]["content"]) for x in f.read_text().splitlines() if x.strip()]
    blob = "\n".join(train)
    in_train = [s for s in sqls if s in blob]
    from verify_pairs import Checker
    ck = Checker()
    bad = []
    for i, (lvl, s, oc) in enumerate(Q, 1):
        try:
            cols, rows = ck.run_sql(s)
            if not rows:
                bad.append((i, "0 rows", s))
            if [c for c in oc if c not in cols]:
                bad.append((i, "order_cols not in output", s))
        except Exception as e:  # noqa: BLE001
            bad.append((i, str(e).splitlines()[0][:200], s))
    print(f"same as eval/dev: {len(same_as_eval)}; inside train: {len(in_train)}; spark problems: {len(bad)}")
    for b in bad:
        print("  ", b)
    if bad or same_as_eval or in_train:
        sys.exit(1)
    out = ROOT / "eval" / "star_probe.jsonl"
    out.write_text("".join(json.dumps({"id": f"star{i:03d}", "level": lvl, "sql": s, "order_cols": oc}) + "\n"
                           for i, (lvl, s, oc) in enumerate(Q, 1)))
    print(f"wrote {len(Q)} queries to {out}")


if __name__ == "__main__":
    main()
