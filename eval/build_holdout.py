"""Hand-written hold-out test set (never used for training).

Every query runs against the *hold-out* tables in sql2pyspark-mlx/schemas.py
(patients, doctors, appointments, flights, bookings), which the dataset
generator never uses, so this set measures generalisation to unseen schemas
and to human-written SQL.

Usage:  python eval/build_holdout.py        -> eval/holdout.jsonl
"""
from __future__ import annotations

import json
from pathlib import Path


def H(level: str, sql: str, body: str, order=()):
    imports = ["from pyspark.sql import functions as F"]
    if "Window." in body:
        imports.append("from pyspark.sql.window import Window")
    return {"level": level, "sql": sql.strip(), "code": "\n".join(imports) + "\n\n" + body.strip(),
            "order_cols": list(order)}


CASES = [
    # ------------------------------------------------------------------ simple
    H("simple", "SELECT id, full_name, city FROM patients WHERE insurance_type = 'PRIVATE';",
      'result = df_patients.filter(F.col("insurance_type") == "PRIVATE").select("id", "full_name", "city")'),
    H("simple", "SELECT * FROM doctors WHERE years_experience >= 10 ORDER BY years_experience DESC;",
      'result = df_doctors.filter(F.col("years_experience") >= 10).orderBy(F.col("years_experience").desc())',
      ["years_experience"]),
    H("simple", "SELECT DISTINCT specialty FROM doctors;",
      'result = df_doctors.select("specialty").distinct()'),
    H("simple", "SELECT id, airline, origin, destination FROM flights WHERE distance_km BETWEEN 1000 AND 5000;",
      'result = df_flights.filter(F.col("distance_km").between(1000, 5000)).select("id", "airline", "origin", "destination")'),
    H("simple", "SELECT id, passenger_name, price FROM bookings WHERE is_cancelled = FALSE AND seat_class IN ('BUSINESS', 'FIRST');",
      """result = (
    df_bookings
    .filter(~F.col("is_cancelled") & F.col("seat_class").isin(["BUSINESS", "FIRST"]))
    .select("id", "passenger_name", "price")
)"""),
    H("simple", "SELECT COUNT(*) AS total_appointments FROM appointments WHERE status = 'NO_SHOW';",
      'result = df_appointments.filter(F.col("status") == "NO_SHOW").agg(F.count("*").alias("total_appointments"))'),
    H("simple", "SELECT id, UPPER(TRIM(full_name)) AS patient_name, YEAR(birth_date) AS birth_year FROM patients;",
      'result = df_patients.select("id", F.upper(F.trim("full_name")).alias("patient_name"), F.year("birth_date").alias("birth_year"))'),
    H("simple", "SELECT id, fee, ROUND(fee * 1.07, 2) AS fee_with_vat FROM appointments;",
      'result = df_appointments.select("id", "fee", F.round(F.col("fee") * 1.07, 2).alias("fee_with_vat"))'),
    H("simple", "SELECT id, departure_time, TO_DATE(departure_time) AS departure_day, HOUR(departure_time) AS departure_hour FROM flights;",
      """result = df_flights.select(
    "id",
    "departure_time",
    F.to_date("departure_time").alias("departure_day"),
    F.hour("departure_time").alias("departure_hour"),
)"""),
    H("simple", "SELECT * FROM bookings WHERE loyalty_tier IS NULL;",
      'result = df_bookings.filter(F.col("loyalty_tier").isNull())'),
    H("simple", "SELECT id, passenger_name FROM bookings WHERE passenger_name LIKE '%a%' ORDER BY id LIMIT 5;",
      'result = df_bookings.filter(F.col("passenger_name").like("%a%")).select("id", "passenger_name").orderBy("id").limit(5)',
      ["id"]),
    H("simple", "SELECT id, doctor_name, COALESCE(hospital, 'Unassigned') AS hospital FROM doctors;",
      'result = df_doctors.select("id", "doctor_name", F.coalesce("hospital", F.lit("Unassigned")).alias("hospital"))'),
    H("simple", """SELECT id,
       CASE WHEN wait_minutes > 60 THEN 'long'
            WHEN wait_minutes > 15 THEN 'normal'
            ELSE 'short' END AS wait_band
FROM appointments;""",
      """result = df_appointments.select(
    "id",
    F.when(F.col("wait_minutes") > 60, "long")
    .when(F.col("wait_minutes") > 15, "normal")
    .otherwise("short")
    .alias("wait_band"),
)"""),
    H("simple", "SELECT MAX(base_fare) AS max_fare, MIN(base_fare) AS min_fare, ROUND(AVG(base_fare), 2) AS avg_fare FROM flights;",
      """result = df_flights.agg(
    F.max("base_fare").alias("max_fare"),
    F.min("base_fare").alias("min_fare"),
    F.round(F.avg("base_fare"), 2).alias("avg_fare"),
)"""),
    H("simple", "SELECT id, DATEDIFF(CURRENT_DATE(), birth_date) / 365 AS approx_age FROM patients;",
      'result = df_patients.select("id", (F.datediff(F.current_date(), "birth_date") / 365).alias("approx_age"))'),
    # ------------------------------------------------------------------ medium
    H("medium", """SELECT specialty, COUNT(*) AS doctor_count, AVG(years_experience) AS avg_experience
FROM doctors
GROUP BY specialty
ORDER BY doctor_count DESC, specialty;""",
      """result = (
    df_doctors
    .groupBy("specialty")
    .agg(F.count("*").alias("doctor_count"), F.avg("years_experience").alias("avg_experience"))
    .orderBy(F.col("doctor_count").desc(), "specialty")
)""", ["doctor_count", "specialty"]),
    H("medium", """SELECT status, COUNT(*) AS n, SUM(fee) AS total_fee
FROM appointments
WHERE appt_date >= '2024-01-01'
GROUP BY status
HAVING COUNT(*) >= 2;""",
      """result = (
    df_appointments
    .filter(F.col("appt_date") >= "2024-01-01")
    .groupBy("status")
    .agg(F.count("*").alias("n"), F.sum("fee").alias("total_fee"))
    .filter(F.col("n") >= 2)
)"""),
    H("medium", "SELECT airline, origin, COUNT(*) AS flights, ROUND(AVG(distance_km), 0) AS avg_distance FROM flights GROUP BY airline, origin;",
      """result = (
    df_flights
    .groupBy("airline", "origin")
    .agg(F.count("*").alias("flights"), F.round(F.avg("distance_km"), 0).alias("avg_distance"))
)"""),
    H("medium", "SELECT seat_class, SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END) AS cancelled, COUNT(*) AS total FROM bookings GROUP BY seat_class;",
      """result = (
    df_bookings
    .groupBy("seat_class")
    .agg(
        F.sum(F.when(F.col("is_cancelled"), 1).otherwise(0)).alias("cancelled"),
        F.count("*").alias("total"),
    )
)"""),
    H("medium", "SELECT a.id, p.full_name, a.appt_date, a.fee FROM appointments a JOIN patients p ON a.patient_id = p.id WHERE a.status = 'DONE';",
      """result = (
    df_appointments.alias("a")
    .join(df_patients.alias("p"), F.col("a.patient_id") == F.col("p.id"), "inner")
    .filter(F.col("a.status") == "DONE")
    .select(F.col("a.id"), F.col("p.full_name"), F.col("a.appt_date"), F.col("a.fee"))
)"""),
    H("medium", "SELECT d.doctor_name, d.specialty, a.appt_date FROM doctors d LEFT JOIN appointments a ON d.id = a.doctor_id WHERE d.specialty = 'Cardiology';",
      """result = (
    df_doctors.alias("d")
    .join(df_appointments.alias("a"), F.col("d.id") == F.col("a.doctor_id"), "left")
    .filter(F.col("d.specialty") == "Cardiology")
    .select(F.col("d.doctor_name"), F.col("d.specialty"), F.col("a.appt_date"))
)"""),
    H("medium", """SELECT b.id, b.passenger_name, f.airline, f.origin, f.destination
FROM bookings b
INNER JOIN flights f ON b.flight_id = f.id
WHERE f.origin = 'BKK'
ORDER BY b.id;""",
      """result = (
    df_bookings.alias("b")
    .join(df_flights.alias("f"), F.col("b.flight_id") == F.col("f.id"), "inner")
    .filter(F.col("f.origin") == "BKK")
    .select(F.col("b.id"), F.col("b.passenger_name"), F.col("f.airline"), F.col("f.origin"), F.col("f.destination"))
    .orderBy("id")
)""", ["id"]),
    H("medium", """SELECT DATE_FORMAT(appt_date, 'yyyy-MM') AS month, COUNT(*) AS appointments
FROM appointments
GROUP BY DATE_FORMAT(appt_date, 'yyyy-MM')
ORDER BY month;""",
      """result = (
    df_appointments
    .groupBy(F.date_format("appt_date", "yyyy-MM").alias("month"))
    .agg(F.count("*").alias("appointments"))
    .orderBy("month")
)""", ["month"]),
    H("medium", "SELECT id, full_name FROM patients WHERE id IN (SELECT patient_id FROM appointments WHERE appt_type = 'emergency');",
      """result = (
    df_patients.alias("p")
    .join(
        df_appointments.filter(F.col("appt_type") == "emergency").alias("a"),
        F.col("p.id") == F.col("a.patient_id"),
        "left_semi",
    )
    .select(F.col("p.id"), F.col("p.full_name"))
)"""),
    H("medium", "SELECT * FROM doctors d WHERE NOT EXISTS (SELECT 1 FROM appointments a WHERE a.doctor_id = d.id AND a.status = 'DONE');",
      """result = (
    df_doctors.alias("d")
    .join(
        df_appointments.filter(F.col("status") == "DONE").alias("a"),
        F.col("d.id") == F.col("a.doctor_id"),
        "left_anti",
    )
)"""),
    H("medium", "SELECT id, flight_id, price, RANK() OVER (PARTITION BY flight_id ORDER BY price DESC) AS price_rank FROM bookings;",
      """w = Window.partitionBy("flight_id").orderBy(F.col("price").desc())

result = df_bookings.select("id", "flight_id", "price", F.rank().over(w).alias("price_rank"))"""),
    H("medium", "SELECT id, patient_id, appt_date, LAG(appt_date) OVER (PARTITION BY patient_id ORDER BY appt_date, id) AS previous_visit FROM appointments;",
      """w = Window.partitionBy("patient_id").orderBy("appt_date", "id")

result = df_appointments.select("id", "patient_id", "appt_date", F.lag("appt_date").over(w).alias("previous_visit"))"""),
    H("medium", "SELECT insurance_type, gender, COUNT(*) AS patients FROM patients GROUP BY insurance_type, gender ORDER BY insurance_type, gender;",
      """result = (
    df_patients
    .groupBy("insurance_type", "gender")
    .agg(F.count("*").alias("patients"))
    .orderBy("insurance_type", "gender")
)""", ["insurance_type", "gender"]),
    H("medium", "SELECT id, airline, base_fare FROM flights WHERE base_fare > (SELECT AVG(base_fare) FROM flights);",
      """avg_fare = df_flights.agg(F.avg("base_fare")).first()[0]

result = df_flights.filter(F.col("base_fare") > avg_fare).select("id", "airline", "base_fare")"""),
    H("medium", "SELECT loyalty_tier, ROUND(AVG(price), 2) AS avg_price FROM bookings WHERE NOT is_cancelled GROUP BY loyalty_tier;",
      """result = (
    df_bookings
    .filter(~F.col("is_cancelled"))
    .groupBy("loyalty_tier")
    .agg(F.round(F.avg("price"), 2).alias("avg_price"))
)"""),
    H("medium", "SELECT city FROM patients WHERE city IS NOT NULL UNION SELECT hospital FROM doctors WHERE hospital IS NOT NULL;",
      """result = (
    df_patients
    .filter(F.col("city").isNotNull())
    .select("city")
    .union(df_doctors.filter(F.col("hospital").isNotNull()).select("hospital"))
    .distinct()
)"""),
    H("medium", "SELECT id, passenger_name, CONCAT(seat_class, '-', COALESCE(loyalty_tier, 'NONE')) AS fare_code FROM bookings;",
      """result = df_bookings.select(
    "id",
    "passenger_name",
    F.concat("seat_class", F.lit("-"), F.coalesce("loyalty_tier", F.lit("NONE"))).alias("fare_code"),
)"""),
    H("medium", """SELECT doctor_id, COUNT(DISTINCT patient_id) AS unique_patients
FROM appointments
GROUP BY doctor_id
HAVING COUNT(DISTINCT patient_id) > 1
ORDER BY unique_patients DESC, doctor_id;""",
      """result = (
    df_appointments
    .groupBy("doctor_id")
    .agg(F.countDistinct("patient_id").alias("unique_patients"))
    .filter(F.col("unique_patients") > 1)
    .orderBy(F.col("unique_patients").desc(), "doctor_id")
)""", ["unique_patients", "doctor_id"]),
    H("medium", "SELECT id, departure_time, DATE_TRUNC('month', departure_time) AS dep_month FROM flights WHERE airline <> 'Delta';",
      """result = (
    df_flights
    .filter(F.col("airline") != "Delta")
    .select("id", "departure_time", F.date_trunc("month", "departure_time").alias("dep_month"))
)"""),
    H("medium", """SELECT origin, destination, COUNT(*) AS n
FROM flights
WHERE destination IS NOT NULL
GROUP BY origin, destination
HAVING COUNT(*) > 1;""",
      """result = (
    df_flights
    .filter(F.col("destination").isNotNull())
    .groupBy("origin", "destination")
    .agg(F.count("*").alias("n"))
    .filter(F.col("n") > 1)
)"""),
    H("medium", """SELECT appt_type,
       COUNT(CASE WHEN status = 'DONE' THEN 1 END) AS done,
       COUNT(CASE WHEN status = 'NO_SHOW' THEN 1 END) AS no_show
FROM appointments
GROUP BY appt_type;""",
      """result = (
    df_appointments
    .groupBy("appt_type")
    .agg(
        F.count(F.when(F.col("status") == "DONE", 1)).alias("done"),
        F.count(F.when(F.col("status") == "NO_SHOW", 1)).alias("no_show"),
    )
)"""),
    H("medium", "SELECT * FROM bookings WHERE booked_at = (SELECT MAX(booked_at) FROM bookings);",
      """max_booked_at = df_bookings.agg(F.max("booked_at")).first()[0]

result = df_bookings.filter(F.col("booked_at") == max_booked_at)"""),
    # ------------------------------------------------------------------ complex
    H("complex", """WITH doctor_revenue AS (
    SELECT doctor_id, SUM(fee) AS revenue, COUNT(*) AS visits
    FROM appointments
    WHERE status = 'DONE'
    GROUP BY doctor_id
)
SELECT d.id, d.doctor_name, d.specialty, r.revenue, r.visits
FROM doctors d
JOIN doctor_revenue r ON d.id = r.doctor_id
ORDER BY r.revenue DESC, d.id;""",
      """doctor_revenue = (
    df_appointments
    .filter(F.col("status") == "DONE")
    .groupBy("doctor_id")
    .agg(F.sum("fee").alias("revenue"), F.count("*").alias("visits"))
)

result = (
    df_doctors.alias("d")
    .join(doctor_revenue.alias("r"), F.col("d.id") == F.col("r.doctor_id"), "inner")
    .select(F.col("d.id"), F.col("d.doctor_name"), F.col("d.specialty"), F.col("r.revenue"), F.col("r.visits"))
    .orderBy(F.col("revenue").desc(), "id")
)""", ["revenue", "id"]),
    H("complex", """WITH ranked AS (
    SELECT b.*, ROW_NUMBER() OVER (PARTITION BY flight_id ORDER BY price DESC, id) AS rn
    FROM bookings b
)
SELECT id, flight_id, passenger_name, price FROM ranked WHERE rn <= 2;""",
      """w = Window.partitionBy("flight_id").orderBy(F.col("price").desc(), "id")

result = (
    df_bookings
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 2)
    .select("id", "flight_id", "passenger_name", "price")
)"""),
    H("complex", """SELECT month, revenue, SUM(revenue) OVER (ORDER BY month) AS cumulative_revenue
FROM (
    SELECT DATE_FORMAT(appt_date, 'yyyy-MM') AS month, SUM(fee) AS revenue
    FROM appointments
    GROUP BY DATE_FORMAT(appt_date, 'yyyy-MM')
) t
ORDER BY month;""",
      """w = Window.orderBy("month")

result = (
    df_appointments
    .groupBy(F.date_format("appt_date", "yyyy-MM").alias("month"))
    .agg(F.sum("fee").alias("revenue"))
    .withColumn("cumulative_revenue", F.sum("revenue").over(w))
    .orderBy("month")
)""", ["month"]),
    H("complex", """SELECT d.specialty,
       SUM(a.fee) AS total_fee,
       ROUND(100.0 * SUM(a.fee) / SUM(SUM(a.fee)) OVER (), 2) AS pct_of_total
FROM appointments a
JOIN doctors d ON a.doctor_id = d.id
GROUP BY d.specialty;""",
      """w = Window.partitionBy()

result = (
    df_appointments.alias("a")
    .join(df_doctors.alias("d"), F.col("a.doctor_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.specialty"))
    .agg(F.sum(F.col("a.fee")).alias("total_fee"))
    .withColumn("pct_of_total", F.round(100.0 * F.col("total_fee") / F.sum("total_fee").over(w), 2))
)"""),
    H("complex", """SELECT p.full_name, d.doctor_name, a.appt_date, a.fee
FROM appointments a
JOIN patients p ON a.patient_id = p.id
JOIN doctors d ON a.doctor_id = d.id
WHERE a.appt_type IN ('surgery', 'emergency') AND d.years_experience > 5;""",
      """result = (
    df_appointments.alias("a")
    .join(df_patients.alias("p"), F.col("a.patient_id") == F.col("p.id"), "inner")
    .join(df_doctors.alias("d"), F.col("a.doctor_id") == F.col("d.id"), "inner")
    .filter(F.col("a.appt_type").isin(["surgery", "emergency"]) & (F.col("d.years_experience") > 5))
    .select(F.col("p.full_name"), F.col("d.doctor_name"), F.col("a.appt_date"), F.col("a.fee"))
)"""),
    H("complex", "SELECT p.id, p.full_name FROM patients p LEFT JOIN appointments a ON p.id = a.patient_id WHERE a.id IS NULL;",
      """result = (
    df_patients.alias("p")
    .join(df_appointments.alias("a"), F.col("p.id") == F.col("a.patient_id"), "left_anti")
    .select(F.col("p.id"), F.col("p.full_name"))
)"""),
    H("complex", """SELECT b.id, b.flight_id, b.price
FROM bookings b
WHERE b.price > (SELECT AVG(b2.price) FROM bookings b2 WHERE b2.flight_id = b.flight_id);""",
      """w = Window.partitionBy("flight_id")

result = (
    df_bookings
    .withColumn("avg_price_flight", F.avg("price").over(w))
    .filter(F.col("price") > F.col("avg_price_flight"))
    .select("id", "flight_id", "price")
)"""),
    H("complex", """SELECT f.airline,
       COUNT(*) AS bookings,
       ROUND(100.0 * SUM(CASE WHEN b.is_cancelled THEN 1 ELSE 0 END) / COUNT(*), 1) AS cancel_rate
FROM bookings b
JOIN flights f ON b.flight_id = f.id
GROUP BY f.airline
ORDER BY cancel_rate DESC, f.airline;""",
      """result = (
    df_bookings.alias("b")
    .join(df_flights.alias("f"), F.col("b.flight_id") == F.col("f.id"), "inner")
    .groupBy(F.col("f.airline"))
    .agg(
        F.count("*").alias("bookings"),
        F.round(100.0 * F.sum(F.when(F.col("b.is_cancelled"), 1).otherwise(0)) / F.count("*"), 1).alias("cancel_rate"),
    )
    .orderBy(F.col("cancel_rate").desc(), "airline")
)""", ["cancel_rate", "airline"]),
    H("complex", """SELECT * FROM (
    SELECT specialty, hospital, years_experience FROM doctors
) PIVOT (
    MAX(years_experience) FOR hospital IN ('City Hospital', 'Bumrungrad', 'General Clinic')
);""",
      """result = (
    df_doctors
    .groupBy("specialty")
    .pivot("hospital", ["City Hospital", "Bumrungrad", "General Clinic"])
    .agg(F.max("years_experience"))
)"""),
    H("complex", """SELECT id, patient_id, appt_date,
       DATEDIFF(appt_date, LAG(appt_date) OVER (PARTITION BY patient_id ORDER BY appt_date, id)) AS days_since_last
FROM appointments
WHERE patient_id IS NOT NULL;""",
      """w = Window.partitionBy("patient_id").orderBy("appt_date", "id")

result = (
    df_appointments
    .filter(F.col("patient_id").isNotNull())
    .select("id", "patient_id", "appt_date", F.datediff("appt_date", F.lag("appt_date").over(w)).alias("days_since_last"))
)"""),
    H("complex", """WITH daily AS (
    SELECT doctor_id, appt_date, COUNT(*) AS n
    FROM appointments
    GROUP BY doctor_id, appt_date
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY doctor_id ORDER BY n DESC) AS r
    FROM daily
)
SELECT doctor_id, appt_date, n FROM ranked WHERE r = 1;""",
      """w = Window.partitionBy("doctor_id").orderBy(F.col("n").desc())

daily = df_appointments.groupBy("doctor_id", "appt_date").agg(F.count("*").alias("n"))

result = (
    daily
    .withColumn("r", F.rank().over(w))
    .filter(F.col("r") == 1)
    .select("doctor_id", "appt_date", "n")
)"""),
    H("complex", """SELECT origin, destination, COUNT(*) AS flights, MAX(base_fare) AS max_fare
FROM flights
WHERE destination IS NOT NULL AND origin <> destination
GROUP BY origin, destination
ORDER BY flights DESC, origin, destination
LIMIT 5;""",
      """result = (
    df_flights
    .filter(F.col("destination").isNotNull() & (F.col("origin") != F.col("destination")))
    .groupBy("origin", "destination")
    .agg(F.count("*").alias("flights"), F.max("base_fare").alias("max_fare"))
    .orderBy(F.col("flights").desc(), "origin", "destination")
    .limit(5)
)""", ["flights", "origin", "destination"]),
    H("complex", """SELECT f.id, f.airline
FROM flights f
WHERE EXISTS (
    SELECT 1 FROM bookings b
    WHERE b.flight_id = f.id AND b.seat_class = 'FIRST' AND NOT b.is_cancelled
);""",
      """result = (
    df_flights.alias("f")
    .join(
        df_bookings.filter((F.col("seat_class") == "FIRST") & ~F.col("is_cancelled")).alias("b"),
        F.col("f.id") == F.col("b.flight_id"),
        "left_semi",
    )
    .select(F.col("f.id"), F.col("f.airline"))
)"""),
    H("complex", """SELECT CASE WHEN YEAR(birth_date) < 2024 THEN 'early'
            WHEN YEAR(birth_date) = 2024 THEN 'mid'
            ELSE 'late' END AS cohort,
       COUNT(*) AS patients
FROM patients
GROUP BY 1
ORDER BY cohort;""",
      """cohort = (
    F.when(F.year("birth_date") < 2024, "early")
    .when(F.year("birth_date") == 2024, "mid")
    .otherwise("late")
)

result = (
    df_patients
    .groupBy(cohort.alias("cohort"))
    .agg(F.count("*").alias("patients"))
    .orderBy("cohort")
)""", ["cohort"]),
    H("complex", "SELECT id, doctor_name FROM doctors WHERE id NOT IN (SELECT doctor_id FROM appointments WHERE status = 'CANCELLED');",
      """result = (
    df_doctors.alias("d")
    .join(
        df_appointments.filter(F.col("status") == "CANCELLED").alias("a"),
        F.col("d.id") == F.col("a.doctor_id"),
        "left_anti",
    )
    .select(F.col("d.id"), F.col("d.doctor_name"))
)"""),
    H("complex", """SELECT 'patient' AS kind, full_name AS name FROM patients
UNION ALL
SELECT 'passenger' AS kind, passenger_name AS name FROM bookings;""",
      """result = (
    df_patients
    .select(F.lit("patient").alias("kind"), F.col("full_name").alias("name"))
    .unionByName(df_bookings.select(F.lit("passenger").alias("kind"), F.col("passenger_name").alias("name")))
)"""),
    H("complex", """SELECT id, departure_time, base_fare,
       ROUND(AVG(base_fare) OVER (PARTITION BY airline ORDER BY departure_time, id
                                  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS fare_ma3
FROM flights;""",
      """w = Window.partitionBy("airline").orderBy("departure_time", "id").rowsBetween(-2, Window.currentRow)

result = df_flights.select("id", "departure_time", "base_fare", F.round(F.avg("base_fare").over(w), 2).alias("fare_ma3"))"""),
    H("complex", "SELECT full_name FROM patients INTERSECT SELECT passenger_name FROM bookings;",
      'result = df_patients.select("full_name").intersect(df_bookings.select("passenger_name"))'),
    H("complex", "SELECT id FROM flights EXCEPT SELECT flight_id FROM bookings;",
      'result = df_flights.select("id").subtract(df_bookings.select("flight_id"))'),
    H("complex", """SELECT *
FROM (
    SELECT patient_id, COUNT(*) AS visits, SUM(fee) AS spend
    FROM appointments
    WHERE patient_id IS NOT NULL
    GROUP BY patient_id
) t
WHERE visits >= 2
ORDER BY spend DESC, patient_id;""",
      """result = (
    df_appointments
    .filter(F.col("patient_id").isNotNull())
    .groupBy("patient_id")
    .agg(F.count("*").alias("visits"), F.sum("fee").alias("spend"))
    .filter(F.col("visits") >= 2)
    .orderBy(F.col("spend").desc(), "patient_id")
)""", ["spend", "patient_id"]),
    H("complex", """SELECT s.specialty, t.appt_type
FROM (SELECT DISTINCT specialty FROM doctors) s
CROSS JOIN (SELECT DISTINCT appt_type FROM appointments WHERE appt_type IS NOT NULL) t
ORDER BY s.specialty, t.appt_type;""",
      """result = (
    df_doctors
    .select("specialty")
    .distinct()
    .crossJoin(df_appointments.filter(F.col("appt_type").isNotNull()).select("appt_type").distinct())
    .orderBy("specialty", "appt_type")
)""", ["specialty", "appt_type"]),
    H("complex", """WITH active AS (
    SELECT * FROM bookings WHERE is_cancelled = FALSE
),
by_flight AS (
    SELECT flight_id, COUNT(*) AS seats_sold, SUM(price) AS revenue
    FROM active
    GROUP BY flight_id
)
SELECT f.id, f.airline, f.origin,
       COALESCE(b.seats_sold, 0) AS seats_sold,
       COALESCE(b.revenue, 0) AS revenue
FROM flights f
LEFT JOIN by_flight b ON f.id = b.flight_id
ORDER BY revenue DESC, f.id;""",
      """active = df_bookings.filter(~F.col("is_cancelled"))

by_flight = (
    active
    .groupBy("flight_id")
    .agg(F.count("*").alias("seats_sold"), F.sum("price").alias("revenue"))
)

result = (
    df_flights.alias("f")
    .join(by_flight.alias("b"), F.col("f.id") == F.col("b.flight_id"), "left")
    .select(
        F.col("f.id"),
        F.col("f.airline"),
        F.col("f.origin"),
        F.coalesce(F.col("b.seats_sold"), F.lit(0)).alias("seats_sold"),
        F.coalesce(F.col("b.revenue"), F.lit(0)).alias("revenue"),
    )
    .orderBy(F.col("revenue").desc(), "id")
)""", ["revenue", "id"]),
    H("complex", "SELECT id, doctor_name, years_experience, PERCENT_RANK() OVER (ORDER BY years_experience) AS pct FROM doctors;",
      """w = Window.orderBy("years_experience")

result = df_doctors.select("id", "doctor_name", "years_experience", F.percent_rank().over(w).alias("pct"))"""),
    H("complex", """SELECT id,
       INITCAP(SUBSTRING_INDEX(TRIM(passenger_name), ' ', 1)) AS first_name,
       LENGTH(TRIM(passenger_name)) AS name_length
FROM bookings
WHERE passenger_name IS NOT NULL;""",
      """result = (
    df_bookings
    .filter(F.col("passenger_name").isNotNull())
    .select(
        "id",
        F.initcap(F.substring_index(F.trim("passenger_name"), " ", 1)).alias("first_name"),
        F.length(F.trim("passenger_name")).alias("name_length"),
    )
)"""),
    H("complex", """SELECT f.airline, b.seat_class, SUM(b.price) AS revenue
FROM bookings b
JOIN flights f ON b.flight_id = f.id
GROUP BY ROLLUP(f.airline, b.seat_class);""",
      """result = (
    df_bookings.alias("b")
    .join(df_flights.alias("f"), F.col("b.flight_id") == F.col("f.id"), "inner")
    .rollup(F.col("f.airline"), F.col("b.seat_class"))
    .agg(F.sum(F.col("b.price")).alias("revenue"))
)"""),
]


def main():
    out = Path(__file__).resolve().parent / "holdout.jsonl"
    with open(out, "w", encoding="utf-8") as f:
        for i, c in enumerate(CASES, 1):
            f.write(json.dumps({"id": f"holdout{i:03d}", **c}, ensure_ascii=False) + "\n")
    print(f"wrote {len(CASES)} cases to {out}")


if __name__ == "__main__":
    main()
