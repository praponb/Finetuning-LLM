"""Dev set eval/dev_short.jsonl (11-23 improvement plan, step A2): 100 short hand-written SQL queries on the 5 hold-out
tables of schemas.py (patients, doctors, appointments, flights, bookings), in the style of eval/holdout.jsonl but new
queries. Used ONLY to choose between model variants / checkpoints; never trained on, never in the promotion rule.

Checks (the script stops on any failure):
  - every query runs in the verify_pairs.Checker Spark session and returns >= 1 row
  - no query equals an eval query (hold-out, v4, fixed, complex test, schema-shift, complex-shift, v11-v14 held-out)
  - no query (normalized) appears inside a train pair of data_q9e or the v18a mix
  python eval/build_dev_short.py        (repo root, with sql2pyspark-mlx/.venv/bin/python)
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MLX = ROOT / "sql2pyspark-mlx"
sys.path.insert(0, str(MLX))
norm = lambda s: " ".join(s.split()).lower().rstrip(";").strip()

S, M, C = "simple", "medium", "complex"
Q: list[tuple[str, str, list[str]]] = [
    # ---- simple (25)
    (S, "SELECT id, doctor_name, hospital FROM doctors WHERE specialty = 'Pediatrics';", []),
    (S, "SELECT * FROM patients WHERE is_smoker = TRUE ORDER BY id;", ["id"]),
    (S, "SELECT DISTINCT airline FROM flights;", []),
    (S, "SELECT id, passenger_name, price FROM bookings WHERE price BETWEEN 500 AND 2000;", []),
    (S, "SELECT id, appt_date, fee FROM appointments WHERE appt_type = 'surgery' AND fee > 1000;", []),
    (S, "SELECT COUNT(*) AS smokers FROM patients WHERE is_smoker;", []),
    (S, "SELECT id, LOWER(TRIM(passenger_name)) AS passenger FROM bookings;", []),
    (S, "SELECT id, distance_km, ROUND(distance_km * 0.621371, 1) AS distance_miles FROM flights;", []),
    (S, "SELECT id, booked_at, MONTH(booked_at) AS booked_month, DAYOFWEEK(booked_at) AS booked_dow FROM bookings;", []),
    (S, "SELECT * FROM patients WHERE city IS NULL;", []),
    (S, "SELECT id, doctor_name FROM doctors WHERE doctor_name LIKE 'Dr. S%' ORDER BY id;", ["id"]),
    (S, "SELECT id, passenger_name, COALESCE(loyalty_tier, 'NONE') AS tier FROM bookings;", []),
    (S, "SELECT id, CASE WHEN base_fare >= 1500 THEN 'premium' WHEN base_fare >= 500 THEN 'standard' ELSE 'budget' END AS fare_band FROM flights;", []),
    (S, "SELECT SUM(fee) AS total_fee, COUNT(*) AS visits, MAX(wait_minutes) AS longest_wait FROM appointments;", []),
    (S, "SELECT id, appt_date, DATE_ADD(appt_date, 30) AS follow_up_due FROM appointments WHERE appt_type = 'checkup';", []),
    (S, "SELECT id, full_name FROM patients WHERE insurance_type <> 'NONE' AND gender = 'F';", []),
    (S, "SELECT id, airline, base_fare FROM flights ORDER BY base_fare DESC, id LIMIT 3;", ["base_fare", "id"]),
    (S, "SELECT id, origin, destination FROM flights WHERE origin IN ('SIN', 'NRT') AND destination <> 'BKK';", []),
    (S, "SELECT id, price, price * 0.9 AS member_price FROM bookings WHERE loyalty_tier = 'GOLD';", []),
    (S, "SELECT id, LENGTH(doctor_name) AS name_len FROM doctors;", []),
    (S, "SELECT id, full_name, birth_date FROM patients WHERE birth_date >= '2024-01-01';", []),
    (S, "SELECT id, status, wait_minutes FROM appointments WHERE wait_minutes IS NOT NULL AND wait_minutes < 30;", []),
    (S, "SELECT COUNT(DISTINCT origin) AS origins, COUNT(DISTINCT destination) AS destinations FROM flights;", []),
    (S, "SELECT id, UPPER(SUBSTRING(seat_class, 1, 1)) AS class_code FROM bookings;", []),
    (S, "SELECT id, specialty, years_experience FROM doctors WHERE NOT (specialty = 'General' OR years_experience < 5);", []),
    # ---- medium (35)
    (M, "SELECT hospital, COUNT(*) AS doctors FROM doctors GROUP BY hospital ORDER BY doctors DESC, hospital;", ["doctors", "hospital"]),
    (M, "SELECT appt_type, ROUND(AVG(wait_minutes), 1) AS avg_wait FROM appointments GROUP BY appt_type;", []),
    (M, "SELECT airline, SUM(distance_km) AS total_km FROM flights GROUP BY airline HAVING SUM(distance_km) > 5000;", []),
    (M, "SELECT seat_class, COUNT(*) AS n, MIN(price) AS cheapest FROM bookings WHERE NOT is_cancelled GROUP BY seat_class;", []),
    (M, "SELECT a.id, d.doctor_name, a.fee FROM appointments a JOIN doctors d ON a.doctor_id = d.id WHERE d.hospital = 'Bumrungrad';", []),
    (M, "SELECT p.full_name, a.appt_date, a.status FROM patients p LEFT JOIN appointments a ON p.id = a.patient_id WHERE p.insurance_type = 'NONE';", []),
    (M, "SELECT b.passenger_name, f.departure_time, b.seat_class FROM bookings b JOIN flights f ON b.flight_id = f.id WHERE f.airline = 'ANA' ORDER BY b.id;", []),
    (M, "SELECT YEAR(appt_date) AS yr, COUNT(*) AS visits FROM appointments GROUP BY YEAR(appt_date) ORDER BY yr;", ["yr"]),
    (M, "SELECT id, full_name FROM patients WHERE id NOT IN (SELECT patient_id FROM appointments WHERE patient_id IS NOT NULL AND status = 'DONE');", []),
    (M, "SELECT * FROM flights f WHERE EXISTS (SELECT 1 FROM bookings b WHERE b.flight_id = f.id AND b.loyalty_tier = 'PLATINUM');", []),
    (M, "SELECT p.* FROM patients p WHERE NOT EXISTS (SELECT 1 FROM appointments a WHERE a.patient_id = p.id AND a.appt_type = 'emergency');", []),
    (M, "SELECT id, doctor_id, fee, DENSE_RANK() OVER (PARTITION BY doctor_id ORDER BY fee DESC) AS fee_rank FROM appointments;", []),
    (M, "SELECT id, flight_id, booked_at, LEAD(booked_at) OVER (PARTITION BY flight_id ORDER BY booked_at, id) AS next_booking FROM bookings;", []),
    (M, "SELECT gender, is_smoker, COUNT(*) AS n FROM patients GROUP BY gender, is_smoker ORDER BY gender, is_smoker;", ["gender", "is_smoker"]),
    (M, "SELECT id, doctor_name, years_experience FROM doctors WHERE years_experience < (SELECT AVG(years_experience) FROM doctors);", []),
    (M, "SELECT airline, ROUND(AVG(base_fare), 2) AS avg_fare FROM flights WHERE origin <> 'JFK' GROUP BY airline;", []),
    (M, "SELECT origin FROM flights UNION SELECT destination FROM flights WHERE destination IS NOT NULL;", []),
    (M, "SELECT id, airline, CONCAT(origin, ' -> ', COALESCE(destination, '???')) AS route FROM flights;", []),
    (M, "SELECT doctor_id, COUNT(*) AS cancelled FROM appointments WHERE status IN ('CANCELLED', 'NO_SHOW') GROUP BY doctor_id HAVING COUNT(*) >= 2 ORDER BY cancelled DESC, doctor_id;", ["cancelled", "doctor_id"]),
    (M, "SELECT id, appt_date, DATE_TRUNC('week', appt_date) AS week_start FROM appointments WHERE status <> 'CANCELLED';", []),
    (M, "SELECT loyalty_tier, COUNT(*) AS bookings, SUM(CASE WHEN seat_class = 'ECONOMY' THEN 1 ELSE 0 END) AS economy FROM bookings GROUP BY loyalty_tier;", []),
    (M, "SELECT status, COUNT(CASE WHEN wait_minutes > 30 THEN 1 END) AS long_waits, COUNT(*) AS total FROM appointments GROUP BY status;", []),
    (M, "SELECT * FROM appointments WHERE fee = (SELECT MAX(fee) FROM appointments);", []),
    (M, "SELECT id, passenger_name FROM bookings WHERE flight_id IN (SELECT id FROM flights WHERE distance_km > 8000);", []),
    (M, "SELECT DATE_FORMAT(booked_at, 'yyyy-MM') AS month, ROUND(SUM(price), 2) AS revenue FROM bookings WHERE NOT is_cancelled GROUP BY DATE_FORMAT(booked_at, 'yyyy-MM') ORDER BY month;", ["month"]),
    (M, "SELECT d.specialty, COUNT(a.id) AS appointments FROM doctors d LEFT JOIN appointments a ON d.id = a.doctor_id GROUP BY d.specialty ORDER BY appointments DESC, d.specialty;", ["appointments", "specialty"]),
    (M, "SELECT id, full_name, city FROM patients WHERE city IN ('Bangkok', 'Chiang Mai') ORDER BY full_name, id;", ["full_name", "id"]),
    (M, "SELECT id, base_fare, NTILE(4) OVER (ORDER BY base_fare, id) AS fare_quartile FROM flights;", []),
    (M, "SELECT airline, MAX(departure_time) AS last_departure FROM flights GROUP BY airline;", []),
    (M, "SELECT id, full_name, CASE gender WHEN 'F' THEN 'female' WHEN 'M' THEN 'male' ELSE 'other' END AS gender_label FROM patients;", []),
    (M, "SELECT patient_id, MIN(appt_date) AS first_visit, MAX(appt_date) AS last_visit FROM appointments WHERE patient_id IS NOT NULL GROUP BY patient_id;", []),
    (M, "SELECT seat_class, ROUND(AVG(price), 0) AS avg_price FROM bookings GROUP BY seat_class HAVING AVG(price) > 1000 ORDER BY avg_price DESC;", ["avg_price"]),
    (M, "SELECT id, doctor_name, REPLACE(doctor_name, 'Dr. ', '') AS surname FROM doctors;", []),
    (M, "SELECT b.id, b.price, f.base_fare, b.price - f.base_fare AS markup FROM bookings b JOIN flights f ON b.flight_id = f.id WHERE NOT b.is_cancelled;", []),
    (M, "SELECT id, appt_date, fee FROM appointments WHERE appt_date BETWEEN '2024-03-01' AND '2024-09-30';", []),
    # ---- complex (40)
    (C, "WITH visits AS (SELECT patient_id, COUNT(*) AS n_visits, SUM(fee) AS total_fee FROM appointments WHERE patient_id IS NOT NULL GROUP BY patient_id) SELECT p.id, p.full_name, v.n_visits, v.total_fee FROM patients p JOIN visits v ON p.id = v.patient_id ORDER BY v.total_fee DESC, p.id;", ["total_fee", "id"]),
    (C, "WITH ranked AS (SELECT a.*, ROW_NUMBER() OVER (PARTITION BY doctor_id ORDER BY fee DESC, id) AS rn FROM appointments a) SELECT id, doctor_id, fee FROM ranked WHERE rn = 1;", []),
    (C, "SELECT airline, COUNT(*) AS flights, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_flights FROM flights GROUP BY airline;", []),
    (C, "SELECT b.seat_class, f.airline, COUNT(*) AS n FROM bookings b JOIN flights f ON b.flight_id = f.id WHERE NOT b.is_cancelled GROUP BY b.seat_class, f.airline HAVING COUNT(*) >= 2;", []),
    (C, "SELECT f.id, f.airline FROM flights f LEFT JOIN bookings b ON f.id = b.flight_id WHERE b.id IS NULL;", []),
    (C, "SELECT a.id, a.doctor_id, a.fee FROM appointments a WHERE a.fee > (SELECT AVG(a2.fee) FROM appointments a2 WHERE a2.doctor_id = a.doctor_id);", []),
    (C, "SELECT p.full_name, d.doctor_name, d.specialty FROM appointments a JOIN patients p ON a.patient_id = p.id JOIN doctors d ON a.doctor_id = d.id WHERE a.status = 'DONE' AND p.is_smoker;", []),
    (C, "SELECT id, appt_date, fee, SUM(fee) OVER (PARTITION BY doctor_id ORDER BY appt_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_fee FROM appointments;", []),
    (C, "SELECT month, bookings, LAG(bookings) OVER (ORDER BY month) AS prev_month FROM (SELECT DATE_FORMAT(booked_at, 'yyyy-MM') AS month, COUNT(*) AS bookings FROM bookings GROUP BY DATE_FORMAT(booked_at, 'yyyy-MM')) t ORDER BY month;", ["month"]),
    (C, "SELECT * FROM (SELECT f.airline, b.seat_class, b.price FROM bookings b JOIN flights f ON b.flight_id = f.id) PIVOT (SUM(price) FOR seat_class IN ('ECONOMY', 'BUSINESS', 'FIRST'));", []),
    (C, "SELECT passenger_name FROM bookings WHERE passenger_name IS NOT NULL EXCEPT SELECT full_name FROM patients;", []),
    (C, "SELECT 'doctor' AS role, doctor_name AS name FROM doctors UNION ALL SELECT 'patient' AS role, full_name AS name FROM patients WHERE full_name IS NOT NULL;", []),
    (C, "SELECT doctor_id, appt_type, COUNT(*) AS n FROM appointments GROUP BY doctor_id, appt_type ORDER BY doctor_id, n DESC, appt_type LIMIT 5;", ["doctor_id", "n", "appt_type"]),
    (C, "WITH fares AS (SELECT airline, AVG(base_fare) AS avg_fare FROM flights GROUP BY airline) SELECT f.id, f.airline, f.base_fare, ROUND(f.base_fare - fa.avg_fare, 2) AS diff_from_avg FROM flights f JOIN fares fa ON f.airline = fa.airline;", []),
    (C, "SELECT d.hospital, d.specialty, SUM(a.fee) AS revenue FROM appointments a JOIN doctors d ON a.doctor_id = d.id WHERE a.status = 'DONE' GROUP BY ROLLUP(d.hospital, d.specialty);", []),
    (C, "SELECT id, passenger_name, price, CUME_DIST() OVER (PARTITION BY seat_class ORDER BY price) AS price_cdf FROM bookings;", []),
    (C, "SELECT CASE WHEN distance_km < 2000 THEN 'short' WHEN distance_km < 6000 THEN 'medium' ELSE 'long' END AS haul, COUNT(*) AS flights, ROUND(AVG(base_fare), 2) AS avg_fare FROM flights GROUP BY 1 ORDER BY haul;", ["haul"]),
    (C, "SELECT d.id, d.doctor_name FROM doctors d WHERE d.id IN (SELECT a.doctor_id FROM appointments a WHERE a.appt_type = 'surgery') AND d.years_experience >= 10;", []),
    (C, "SELECT DISTINCT p.city FROM patients p JOIN appointments a ON p.id = a.patient_id WHERE a.fee > 1500 AND p.city IS NOT NULL;", []),
    (C, "WITH last_appt AS (SELECT patient_id, MAX(appt_date) AS last_date FROM appointments GROUP BY patient_id) SELECT a.id, a.patient_id, a.appt_date, a.status FROM appointments a JOIN last_appt l ON a.patient_id = l.patient_id AND a.appt_date = l.last_date;", []),
    (C, "SELECT id, departure_time, base_fare, ROUND(AVG(base_fare) OVER (PARTITION BY origin ORDER BY departure_time, id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), 2) AS centered_avg FROM flights;", []),
    (C, "SELECT f.origin, COUNT(DISTINCT b.passenger_name) AS passengers FROM flights f JOIN bookings b ON f.id = b.flight_id GROUP BY f.origin HAVING COUNT(DISTINCT b.passenger_name) > 2 ORDER BY passengers DESC, f.origin;", ["passengers", "origin"]),
    (C, "SELECT id, full_name, CONCAT(UPPER(SUBSTRING(TRIM(full_name), 1, 1)), '.', COALESCE(city, 'unknown')) AS tag FROM patients;", []),
    (C, "SELECT b.id, b.passenger_name FROM bookings b WHERE NOT EXISTS (SELECT 1 FROM flights f WHERE f.id = b.flight_id AND f.airline = 'Delta') AND b.seat_class <> 'ECONOMY';", []),
    (C, "SELECT a.doctor_id, ROUND(100.0 * SUM(CASE WHEN a.status = 'NO_SHOW' THEN 1 ELSE 0 END) / COUNT(*), 1) AS no_show_rate FROM appointments a GROUP BY a.doctor_id ORDER BY no_show_rate DESC, a.doctor_id;", ["no_show_rate", "doctor_id"]),
    (C, "SELECT x.appt_type, y.seat_class FROM (SELECT DISTINCT appt_type FROM appointments WHERE appt_type IS NOT NULL) x CROSS JOIN (SELECT DISTINCT seat_class FROM bookings) y;", []),
    (C, "WITH stats AS (SELECT specialty, MAX(years_experience) AS max_exp FROM doctors GROUP BY specialty) SELECT d.id, d.doctor_name, d.specialty FROM doctors d JOIN stats s ON d.specialty = s.specialty AND d.years_experience = s.max_exp;", []),
    (C, "SELECT b.id, b.passenger_name, DATEDIFF(TO_DATE(f.departure_time), TO_DATE(b.booked_at)) AS days_before FROM bookings b JOIN flights f ON b.flight_id = f.id WHERE NOT b.is_cancelled;", []),
    (C, "SELECT insurance_type, COUNT(*) AS patients, SUM(CASE WHEN is_smoker THEN 1 ELSE 0 END) AS smokers, ROUND(AVG(YEAR(birth_date)), 0) AS avg_birth_year FROM patients GROUP BY insurance_type ORDER BY insurance_type;", ["insurance_type"]),
    (C, "SELECT * FROM (SELECT seat_class, loyalty_tier, price FROM bookings WHERE loyalty_tier IS NOT NULL) PIVOT (COUNT(price) FOR loyalty_tier IN ('SILVER', 'GOLD', 'PLATINUM'));", []),
    (C, "SELECT d.doctor_name, COUNT(DISTINCT a.patient_id) AS patients, ROUND(AVG(a.wait_minutes), 1) AS avg_wait FROM doctors d JOIN appointments a ON d.id = a.doctor_id GROUP BY d.doctor_name HAVING COUNT(*) >= 3;", []),
    (C, "SELECT id, airline, departure_time, FIRST_VALUE(base_fare) OVER (PARTITION BY airline ORDER BY departure_time, id) AS first_fare FROM flights;", []),
    (C, "SELECT full_name FROM patients WHERE full_name IS NOT NULL INTERSECT SELECT passenger_name FROM bookings WHERE seat_class <> 'FIRST';", []),
    (C, "WITH monthly AS (SELECT doctor_id, DATE_FORMAT(appt_date, 'yyyy-MM') AS month, SUM(fee) AS revenue FROM appointments GROUP BY doctor_id, DATE_FORMAT(appt_date, 'yyyy-MM')) SELECT doctor_id, month, revenue, revenue - LAG(revenue) OVER (PARTITION BY doctor_id ORDER BY month) AS change FROM monthly;", []),
    (C, "SELECT p.id, p.full_name, COUNT(a.id) AS visits FROM patients p LEFT JOIN appointments a ON p.id = a.patient_id AND a.status = 'DONE' GROUP BY p.id, p.full_name ORDER BY visits DESC, p.id LIMIT 10;", ["visits", "id"]),
    (C, "SELECT b.flight_id, MAX(CASE WHEN b.seat_class = 'FIRST' THEN b.price END) AS first_max, MAX(CASE WHEN b.seat_class = 'ECONOMY' THEN b.price END) AS economy_max FROM bookings b GROUP BY b.flight_id;", []),
    (C, "SELECT f.airline, f.origin, SUM(b.price) AS revenue FROM flights f JOIN bookings b ON f.id = b.flight_id WHERE NOT b.is_cancelled GROUP BY CUBE(f.airline, f.origin);", []),
    (C, "SELECT a.id, a.fee, a.status FROM appointments a JOIN doctors d ON a.doctor_id = d.id AND d.specialty IN ('Cardiology', 'Orthopedics') WHERE a.fee BETWEEN 100 AND 1500;", []),
    (C, "SELECT id, INITCAP(LOWER(TRIM(full_name))) AS clean_name, REGEXP_REPLACE(TRIM(full_name), ' +', '_') AS slug FROM patients WHERE full_name IS NOT NULL;", []),
    (C, "SELECT doctor_id, appt_date, n FROM (SELECT doctor_id, appt_date, COUNT(*) AS n, MAX(COUNT(*)) OVER (PARTITION BY doctor_id) AS max_n FROM appointments GROUP BY doctor_id, appt_date) t WHERE n = max_n;", []),
]


def eval_sql() -> set[str]:
    evs = [ROOT / "eval" / f for f in ("holdout.jsonl", "holdout_v4.jsonl", "test_fixed.jsonl", "complex_v8_test.jsonl",
                                        "real_queries.jsonl", "harness_suite.jsonl")]
    evs += [MLX / "complex_v16/data/complex_shift_meta.jsonl", MLX / "schema_v15/data/schema_shift_meta.jsonl"]
    evs += [MLX / f"targeted_v{v}/data/heldout_meta.jsonl" for v in (11, 12, 13, 14)]
    out = set()
    for f in evs:
        for x in f.read_text().splitlines():
            if x.strip():
                d = json.loads(x)
                out.add(norm(d.get("sql") or d.get("input") or ""))
    return out


def main() -> None:
    assert len(Q) == 100, len(Q)
    sqls = [norm(s) for _, s, _ in Q]
    assert len(set(sqls)) == 100, "duplicate dev queries"
    same_as_eval = [s for s in sqls if s in eval_sql()]
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
            missing = [c for c in oc if c not in cols]
            if missing:
                bad.append((i, f"order_cols not in output {missing}", s))
        except Exception as e:  # noqa: BLE001
            bad.append((i, str(e).splitlines()[0][:200], s))
    print(f"same as eval: {len(same_as_eval)}; inside train: {len(in_train)}; spark problems: {len(bad)}")
    for b in bad:
        print("  ", b)
    for s in same_as_eval + in_train:
        print("   leak:", s)
    if bad or same_as_eval or in_train:
        sys.exit(1)
    out = ROOT / "eval" / "dev_short.jsonl"
    out.write_text("".join(json.dumps({"id": f"dev{i:03d}", "level": lvl, "sql": s, "order_cols": oc}) + "\n"
                           for i, (lvl, s, oc) in enumerate(Q, 1)))
    print(f"wrote {out} (100 queries)")


if __name__ == "__main__":
    main()
