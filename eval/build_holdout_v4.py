"""Hand-written hold-out set for the v4 features (never used for training).

Like eval/holdout.jsonl, every query runs on the *hold-out* tables (patients, doctors, appointments,
flights, bookings) plus the nested hold-out table vehicle_trips, so it measures generalisation to unseen
schemas. Each query targets features that v1-v3 never saw in training: GROUPING SETS, INTERVAL arithmetic,
NULLS FIRST/LAST, QUALIFY, sessionization, RANGE frames, FILTER clauses, multi-aggregate PIVOT, UNPIVOT,
LATERAL joins, EXCEPT/INTERSECT ALL, correlated scalar subqueries, nested data (structs, arrays of
structs, maps, JSON) with higher-order lambdas, and multi-CTE pipelines.

`level` is the feature group, so results can be broken down by group.
Every reference answer is checked against its SQL in Spark before the set is written.

Usage:  python eval/build_holdout_v4.py        -> eval/holdout_v4.jsonl
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "sql2pyspark-mlx"))


def H(level: str, sql: str, body: str, order=()):
    imports = ["from pyspark.sql import functions as F"]
    if "Window." in body:
        imports.append("from pyspark.sql.window import Window")
    return {"level": level, "sql": sql.strip(), "code": "\n".join(imports) + "\n\n" + body.strip(),
            "order_cols": list(order)}


CASES = [
    # ------------------------------------------------------------------ aggregation
    H("aggregation", """
SELECT d.specialty, a.status, COUNT(*) AS appointments, ROUND(SUM(a.fee), 2) AS total_fee
FROM appointments a
JOIN doctors d ON a.doctor_id = d.id
GROUP BY GROUPING SETS ((d.specialty, a.status), (d.specialty), ())
""", """
result = (
    df_appointments.alias("a")
    .join(df_doctors.alias("d"), F.col("a.doctor_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["specialty", "status"], ["specialty"], []],
        F.col("d.specialty").alias("specialty"),
        F.col("a.status").alias("status"),
    )
    .agg(F.count("*").alias("appointments"), F.round(F.sum(F.col("a.fee")), 2).alias("total_fee"))
)
"""),
    H("aggregation", """
SELECT airline, origin, GROUPING_ID() AS grouping_level, COUNT(*) AS flights, MAX(base_fare) AS max_fare
FROM flights
GROUP BY ROLLUP (airline, origin)
HAVING COUNT(*) >= 2
ORDER BY grouping_level, flights DESC
""", """
result = (
    df_flights
    .rollup("airline", "origin")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("flights"),
        F.max("base_fare").alias("max_fare"),
    )
    .filter(F.col("flights") >= 2)
    .orderBy("grouping_level", F.col("flights").desc())
)
""", ["grouping_level", "flights"]),
    H("aggregation", """
SELECT doctor_id,
       COUNT(*) FILTER (WHERE status = 'NO_SHOW') AS no_shows,
       AVG(wait_minutes) FILTER (WHERE appt_type = 'emergency') AS avg_emergency_wait,
       COUNT_IF(fee > 1000) AS expensive_visits,
       ROUND(SUM(fee) / NULLIF(COUNT(CASE WHEN status = 'DONE' THEN 1 END), 0), 2) AS fee_per_done_visit
FROM appointments
GROUP BY doctor_id
""", """
result = (
    df_appointments
    .groupBy("doctor_id")
    .agg(
        F.count(F.when(F.col("status") == "NO_SHOW", 1)).alias("no_shows"),
        F.avg(F.when(F.col("appt_type") == "emergency", F.col("wait_minutes"))).alias("avg_emergency_wait"),
        F.count_if(F.col("fee") > 1000).alias("expensive_visits"),
        F.round(F.sum("fee") / F.nullif(F.count(F.when(F.col("status") == "DONE", 1)), F.lit(0)), 2)
        .alias("fee_per_done_visit"),
    )
)
"""),
    H("aggregation", """
SELECT seat_class,
       PERCENTILE_APPROX(price, 0.5) AS median_price,
       STDDEV_SAMP(price) AS price_stddev,
       SORT_ARRAY(COLLECT_SET(loyalty_tier)) AS tiers,
       PERCENT_RANK() OVER (ORDER BY AVG(price) DESC) AS price_rank
FROM bookings
WHERE NOT is_cancelled
GROUP BY seat_class
""", """
w = Window.orderBy(F.col("avg_price").desc())

result = (
    df_bookings
    .filter(~F.col("is_cancelled"))
    .groupBy("seat_class")
    .agg(
        F.percentile_approx("price", 0.5).alias("median_price"),
        F.stddev_samp("price").alias("price_stddev"),
        F.sort_array(F.collect_set("loyalty_tier")).alias("tiers"),
        F.avg("price").alias("avg_price"),
    )
    .withColumn("price_rank", F.percent_rank().over(w))
    .drop("avg_price")
)
"""),
    # ------------------------------------------------------------------ windows
    H("window", """
SELECT id, full_name, city, birth_date,
       DENSE_RANK() OVER (PARTITION BY city ORDER BY birth_date DESC NULLS LAST) AS age_rank_in_city,
       LAST_VALUE(insurance_type) IGNORE NULLS OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_insurance
FROM patients
ORDER BY city NULLS FIRST, age_rank_in_city
""", """
w = Window.partitionBy("city").orderBy(F.col("birth_date").desc_nulls_last())
w2 = Window.orderBy("id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_patients
    .select(
        "id",
        "full_name",
        "city",
        "birth_date",
        F.dense_rank().over(w).alias("age_rank_in_city"),
        F.last("insurance_type", ignorenulls=True).over(w2).alias("last_known_insurance"),
    )
    .orderBy(F.col("city").asc_nulls_first(), "age_rank_in_city")
)
""", ["city", "age_rank_in_city"]),
    H("window", """
SELECT id, origin, departure_time, base_fare,
       COUNT(*) OVER (PARTITION BY origin ORDER BY CAST(departure_time AS BIGINT)
                      RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS flights_last_30d,
       AVG(base_fare) OVER (PARTITION BY origin ORDER BY departure_time, id
                            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS avg_fare_prev_3
FROM flights
""", """
w = Window.partitionBy("origin").orderBy(F.col("departure_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy("origin").orderBy("departure_time", "id").rowsBetween(-3, -1)

result = (
    df_flights
    .select(
        "id",
        "origin",
        "departure_time",
        "base_fare",
        F.count("*").over(w).alias("flights_last_30d"),
        F.avg("base_fare").over(w2).alias("avg_fare_prev_3"),
    )
)
"""),
    H("window", """
SELECT id, doctor_id, fee,
       SUM(fee) OVER w_doc AS running_fee,
       LAG(fee, 1, 0) OVER w_doc AS previous_fee,
       CUME_DIST() OVER (PARTITION BY doctor_id ORDER BY fee) AS fee_cume_dist
FROM appointments
WINDOW w_doc AS (PARTITION BY doctor_id ORDER BY appt_date, id)
""", """
w_doc = Window.partitionBy("doctor_id").orderBy("appt_date", "id")
w = Window.partitionBy("doctor_id").orderBy("fee")

result = (
    df_appointments
    .select(
        "id",
        "doctor_id",
        "fee",
        F.sum("fee").over(w_doc).alias("running_fee"),
        F.lag("fee", 1, 0).over(w_doc).alias("previous_fee"),
        F.cume_dist().over(w).alias("fee_cume_dist"),
    )
)
"""),
    H("window", """
SELECT id, patient_id, doctor_id, appt_date, fee
FROM appointments
WHERE patient_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY appt_date DESC, id DESC) = 1
""", """
w = Window.partitionBy("patient_id").orderBy(F.col("appt_date").desc(), F.col("id").desc())

result = (
    df_appointments
    .filter(F.col("patient_id").isNotNull())
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "patient_id", "doctor_id", "appt_date", "fee")
)
"""),
    H("window", """
WITH gaps AS (
    SELECT id, driver_id, trip_start,
           CASE WHEN LAG(trip_start) OVER (PARTITION BY driver_id ORDER BY trip_start, id) IS NULL
                  OR trip_start > LAG(trip_start) OVER (PARTITION BY driver_id ORDER BY trip_start, id) + INTERVAL '6' HOUR
                THEN 1 ELSE 0 END AS is_new_shift
    FROM vehicle_trips
)
SELECT id, driver_id, trip_start,
       SUM(is_new_shift) OVER (PARTITION BY driver_id ORDER BY trip_start, id
                               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS shift_no
FROM gaps
""", """
w = Window.partitionBy("driver_id").orderBy("trip_start", "id")
w2 = Window.partitionBy("driver_id").orderBy("trip_start", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_vehicle_trips
    .select(
        "id",
        "driver_id",
        "trip_start",
        F.when(
            F.lag("trip_start").over(w).isNull()
            | (F.col("trip_start") > F.lag("trip_start").over(w) + F.expr("INTERVAL 6 HOURS")),
            1,
        ).otherwise(0).alias("is_new_shift"),
    )
)

result = gaps.select("id", "driver_id", "trip_start", F.sum("is_new_shift").over(w2).alias("shift_no"))
"""),
    # ------------------------------------------------------------------ joins
    H("join", """
SELECT b.id AS booking_id, b.passenger_name, f.airline, f.departure_time
FROM bookings b
JOIN flights f
  ON b.flight_id = f.id
 AND b.booked_at >= f.departure_time - INTERVAL 365 DAYS
 AND b.booked_at < f.departure_time + INTERVAL '30' DAY
""", """
result = (
    df_bookings.alias("b")
    .join(
        df_flights.alias("f"),
        (F.col("b.flight_id") == F.col("f.id"))
        & (F.col("b.booked_at") >= F.col("f.departure_time") - F.expr("INTERVAL 365 DAYS"))
        & (F.col("b.booked_at") < F.col("f.departure_time") + F.expr("INTERVAL 30 DAYS")),
        "inner",
    )
    .select(
        F.col("b.id").alias("booking_id"),
        F.col("b.passenger_name"),
        F.col("f.airline"),
        F.col("f.departure_time"),
    )
)
"""),
    H("join", """
SELECT d.id, d.doctor_name, latest.appt_date AS last_appt_date, latest.fee AS last_fee
FROM doctors d
LEFT JOIN LATERAL (
    SELECT a.appt_date, a.fee
    FROM appointments a
    WHERE a.doctor_id = d.id
    ORDER BY a.appt_date DESC, a.id DESC
    LIMIT 1
) latest ON TRUE
""", """
w = Window.partitionBy(F.col("d.id")).orderBy(F.col("a.appt_date").desc(), F.col("a.id").desc())

result = (
    df_doctors.alias("d")
    .join(df_appointments.alias("a"), F.col("a.doctor_id") == F.col("d.id"), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        F.col("d.id"),
        F.col("d.doctor_name"),
        F.col("a.appt_date").alias("last_appt_date"),
        F.col("a.fee").alias("last_fee"),
    )
)
"""),
    H("join", """
SELECT p.id, p.full_name, p.city
FROM patients p
WHERE NOT EXISTS (
    SELECT 1 FROM appointments a
    WHERE a.patient_id = p.id
      AND a.status = 'DONE'
      AND a.appt_date >= p.birth_date
)
""", """
result = (
    df_patients.alias("p")
    .join(
        df_appointments.alias("a"),
        (F.col("a.patient_id") == F.col("p.id"))
        & (F.col("a.status") == "DONE")
        & (F.col("a.appt_date") >= F.col("p.birth_date")),
        "left_anti",
    )
    .select("id", "full_name", "city")
)
"""),
    H("join", """
SELECT COALESCE(d.hospital, 'Unknown') AS hospital, p.city, COUNT(*) AS visits
FROM appointments a
JOIN doctors d ON a.doctor_id = d.id
JOIN patients p ON a.patient_id <=> p.id
GROUP BY COALESCE(d.hospital, 'Unknown'), p.city
ORDER BY visits DESC, hospital
""", """
result = (
    df_appointments.alias("a")
    .join(df_doctors.alias("d"), F.col("a.doctor_id") == F.col("d.id"), "inner")
    .join(df_patients.alias("p"), F.col("a.patient_id").eqNullSafe(F.col("p.id")), "inner")
    .groupBy(F.coalesce(F.col("d.hospital"), F.lit("Unknown")).alias("hospital"), F.col("p.city"))
    .agg(F.count("*").alias("visits"))
    .orderBy(F.col("visits").desc(), "hospital")
)
""", ["visits", "hospital"]),
    H("join", """
SELECT d.id, d.doctor_name,
       (SELECT MAX(a.fee) FROM appointments a WHERE a.doctor_id = d.id) AS max_fee,
       CASE WHEN d.hospital RLIKE '^(City|General)' THEN 'PUBLIC' ELSE 'PRIVATE' END AS hospital_type
FROM doctors d
ORDER BY max_fee DESC NULLS LAST
""", """
max_fee_by_doctor = df_appointments.groupBy("doctor_id").agg(F.max("fee").alias("max_fee"))

result = (
    df_doctors
    .join(max_fee_by_doctor, df_doctors["id"] == max_fee_by_doctor["doctor_id"], "left")
    .select(
        "id",
        "doctor_name",
        "max_fee",
        F.when(F.col("hospital").rlike("^(City|General)"), "PUBLIC").otherwise("PRIVATE").alias("hospital_type"),
    )
    .orderBy(F.col("max_fee").desc_nulls_last())
)
""", ["max_fee"]),
    # ------------------------------------------------------------------ set operations
    H("setop", """
SELECT city, 'patient' AS source FROM patients WHERE city IS NOT NULL
UNION
SELECT hospital, 'doctor' AS source FROM doctors WHERE hospital IS NOT NULL
ORDER BY source, city
""", """
result = (
    df_patients
    .filter(F.col("city").isNotNull())
    .select("city", F.lit("patient").alias("source"))
    .union(df_doctors.filter(F.col("hospital").isNotNull()).select("hospital", F.lit("doctor").alias("source")))
    .distinct()
    .orderBy("source", "city")
)
""", ["source", "city"]),
    H("setop", """
SELECT patient_id, doctor_id FROM appointments WHERE status = 'DONE'
EXCEPT ALL
SELECT patient_id, doctor_id FROM appointments WHERE appt_type = 'checkup'
""", """
result = (
    df_appointments
    .filter(F.col("status") == "DONE")
    .select("patient_id", "doctor_id")
    .exceptAll(df_appointments.filter(F.col("appt_type") == "checkup").select("patient_id", "doctor_id"))
)
"""),
    H("setop", """
SELECT flight_id FROM bookings WHERE seat_class IN ('BUSINESS', 'FIRST')
INTERSECT ALL
SELECT flight_id FROM bookings WHERE NOT is_cancelled
""", """
result = (
    df_bookings
    .filter(F.col("seat_class").isin(["BUSINESS", "FIRST"]))
    .select("flight_id")
    .intersectAll(df_bookings.filter(~F.col("is_cancelled")).select("flight_id"))
)
"""),
    # ------------------------------------------------------------------ reshape
    H("reshape", """
SELECT *
FROM (SELECT doctor_id, appt_type, fee FROM appointments) src
PIVOT (
    SUM(fee) AS total_fee,
    COUNT(1) AS visits
    FOR appt_type IN ('checkup' AS checkup, 'emergency' AS emergency)
)
""", """
result = (
    df_appointments
    .groupBy("doctor_id")
    .pivot("appt_type", ["checkup", "emergency"])
    .agg(F.sum("fee").alias("total_fee"), F.count(F.lit(1)).alias("visits"))
)
"""),
    H("reshape", """
SELECT *
FROM (
    SELECT id, airline, CAST(distance_km AS DOUBLE) AS distance, base_fare AS fare
    FROM flights
) src
UNPIVOT (measure_value FOR measure IN (distance AS DISTANCE_KM, fare AS BASE_FARE))
""", """
result = (
    df_flights
    .select("id", "airline", F.col("distance_km").cast("double").alias("DISTANCE_KM"), F.col("base_fare").alias("BASE_FARE"))
    .unpivot(["id", "airline"], ["DISTANCE_KM", "BASE_FARE"], "measure", "measure_value")
    .filter(F.col("measure_value").isNotNull())
)
"""),
    # ------------------------------------------------------------------ nested data
    H("nested", """
SELECT t.id, t.driver_id, stop.stop_name, stop.wait_min, stop.fare
FROM vehicle_trips t
LATERAL VIEW OUTER EXPLODE(t.stops) s AS stop
""", """
result = (
    df_vehicle_trips
    .withColumn("stop", F.explode_outer("stops"))
    .select("id", "driver_id", F.col("stop.stop_name"), F.col("stop.wait_min"), F.col("stop.fare"))
)
"""),
    H("nested", """
SELECT id, pos + 1 AS stop_number, s.stop_name
FROM vehicle_trips
LATERAL VIEW POSEXPLODE(stops) x AS pos, s
WHERE s.wait_min > 5
""", """
result = (
    df_vehicle_trips
    .select("id", F.posexplode("stops").alias("pos", "s"))
    .filter(F.col("s.wait_min") > 5)
    .select("id", (F.col("pos") + 1).alias("stop_number"), F.col("s.stop_name"))
)
"""),
    H("nested", """
SELECT id,
       SIZE(stops) AS num_stops,
       AGGREGATE(stops, 0D, (acc, s) -> acc + s.fare, acc -> ROUND(acc, 2)) AS stop_fares,
       TRANSFORM(FILTER(stops, s -> s.wait_min >= 10), s -> s.stop_name) AS long_wait_stops,
       EXISTS(stops, s -> s.stop_name = 'Airport') AS via_airport,
       ZIP_WITH(fare_components, surcharge_rates, (f, r) -> ROUND(f * r, 2)) AS charged_components
FROM vehicle_trips
""", """
result = df_vehicle_trips.select(
    "id",
    F.size("stops").alias("num_stops"),
    F.aggregate("stops", F.lit(0.0), lambda acc, s: acc + s["fare"], lambda acc: F.round(acc, 2)).alias("stop_fares"),
    F.transform(F.filter("stops", lambda s: s["wait_min"] >= 10), lambda s: s["stop_name"]).alias("long_wait_stops"),
    F.exists("stops", lambda s: s["stop_name"] == "Airport").alias("via_airport"),
    F.zip_with("fare_components", "surcharge_rates", lambda f, r: F.round(f * r, 2)).alias("charged_components"),
)
"""),
    H("nested", """
SELECT id,
       route.origin,
       COALESCE(route.destination, 'OPEN') AS destination,
       extras['vehicle_type'] AS vehicle_type,
       TRY_CAST(extras['rating'] AS DOUBLE) AS rating,
       GET_JSON_OBJECT(telemetry_json, '$.speed.max') AS max_speed_text,
       FROM_JSON(telemetry_json, 'STRUCT<speed: STRUCT<avg: DOUBLE, max: DOUBLE>, battery: INT>').battery AS battery
FROM vehicle_trips
""", """
telemetry_schema = "STRUCT<speed: STRUCT<avg: DOUBLE, max: DOUBLE>, battery: INT>"

result = df_vehicle_trips.select(
    "id",
    F.col("route.origin"),
    F.coalesce(F.col("route.destination"), F.lit("OPEN")).alias("destination"),
    F.col("extras")["vehicle_type"].alias("vehicle_type"),
    F.col("extras")["rating"].try_cast("double").alias("rating"),
    F.get_json_object("telemetry_json", "$.speed.max").alias("max_speed_text"),
    F.from_json("telemetry_json", telemetry_schema)["battery"].alias("battery"),
)
"""),
    # ------------------------------------------------------------------ pipelines
    H("pipeline", """
WITH recent AS (
    -- appointments with a known patient
    SELECT *
    FROM appointments
    WHERE patient_id IS NOT NULL
      AND appt_date >= DATE '2023-06-01'
),
ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY appt_date, id) AS visit_no,
           SUM(fee) OVER (PARTITION BY patient_id ORDER BY appt_date, id
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_fee
    FROM recent
),
per_doctor AS (
    SELECT doctor_id,
           COUNT(*) AS visits,
           COUNT(*) FILTER (WHERE visit_no = 1) AS first_visits,
           MAX(cumulative_fee) AS max_patient_spend
    FROM ranked
    GROUP BY doctor_id
)
SELECT d.doctor_name, d.specialty, p.visits, p.first_visits, p.max_patient_spend,
       RANK() OVER (PARTITION BY d.specialty ORDER BY p.visits DESC) AS rank_in_specialty
FROM per_doctor p
JOIN doctors d ON p.doctor_id = d.id
ORDER BY d.specialty, rank_in_specialty
""", """
w = Window.partitionBy("patient_id").orderBy("appt_date", "id")
w2 = Window.partitionBy("patient_id").orderBy("appt_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("specialty").orderBy(F.col("visits").desc())

recent = (
    df_appointments
    .filter(F.col("patient_id").isNotNull() & (F.col("appt_date") >= F.lit("2023-06-01").cast("date")))
)

ranked = (
    recent
    .withColumn("visit_no", F.row_number().over(w))
    .withColumn("cumulative_fee", F.sum("fee").over(w2))
)

per_doctor = (
    ranked
    .groupBy("doctor_id")
    .agg(
        F.count("*").alias("visits"),
        F.count(F.when(F.col("visit_no") == 1, 1)).alias("first_visits"),
        F.max("cumulative_fee").alias("max_patient_spend"),
    )
)

result = (
    per_doctor.alias("p")
    .join(df_doctors.alias("d"), F.col("p.doctor_id") == F.col("d.id"), "inner")
    .select(
        F.col("d.doctor_name"),
        F.col("d.specialty"),
        F.col("p.visits"),
        F.col("p.first_visits"),
        F.col("p.max_patient_spend"),
    )
    .withColumn("rank_in_specialty", F.rank().over(w3))
    .orderBy("specialty", "rank_in_specialty")
)
""", ["specialty", "rank_in_specialty"]),
    H("pipeline", """
WITH stops AS (
    SELECT t.id AS trip_id, t.driver_id, t.trip_start, s.stop_name, s.wait_min, s.fare
    FROM vehicle_trips t
    LATERAL VIEW EXPLODE(t.stops) x AS s
),
per_driver AS (
    SELECT driver_id,
           DATE_TRUNC('DAY', trip_start) AS trip_day,
           COUNT(DISTINCT trip_id) AS trips,
           SUM(fare) AS stop_revenue,
           AVG(wait_min) FILTER (WHERE stop_name = 'Airport') AS airport_wait
    FROM stops
    GROUP BY driver_id, DATE_TRUNC('DAY', trip_start)
)
SELECT driver_id, trip_day, trips, ROUND(stop_revenue, 2) AS stop_revenue, airport_wait,
       DENSE_RANK() OVER (PARTITION BY trip_day ORDER BY stop_revenue DESC) AS revenue_rank
FROM per_driver
ORDER BY trip_day, revenue_rank, driver_id
""", """
w = Window.partitionBy("trip_day").orderBy(F.col("stop_revenue").desc())

stops = (
    df_vehicle_trips
    .withColumn("s", F.explode("stops"))
    .select(
        F.col("id").alias("trip_id"),
        "driver_id",
        "trip_start",
        F.col("s.stop_name"),
        F.col("s.wait_min"),
        F.col("s.fare"),
    )
)

per_driver = (
    stops
    .groupBy("driver_id", F.date_trunc("DAY", "trip_start").alias("trip_day"))
    .agg(
        F.countDistinct("trip_id").alias("trips"),
        F.sum("fare").alias("stop_revenue"),
        F.avg(F.when(F.col("stop_name") == "Airport", F.col("wait_min"))).alias("airport_wait"),
    )
)

result = (
    per_driver
    .select(
        "driver_id",
        "trip_day",
        "trips",
        F.round("stop_revenue", 2).alias("stop_revenue"),
        "airport_wait",
        F.dense_rank().over(w).alias("revenue_rank"),
    )
    .orderBy("trip_day", "revenue_rank", "driver_id")
)
""", ["trip_day", "revenue_rank", "driver_id"]),
]


def main():
    from verify_pairs import Checker

    checker = Checker()
    out = HERE / "holdout_v4.jsonl"
    bad = 0
    with open(out, "w", encoding="utf-8") as f:
        for i, c in enumerate(CASES, 1):
            res = checker.check(c["sql"], c["code"], c["order_cols"])
            if not res.ok or not res.n_rows:
                print(f"  holdout_v4_{i:03d} ({c['level']}): FAILED {res}")
                bad += 1
                continue
            f.write(json.dumps({"id": f"holdout_v4_{i:03d}", **c}, ensure_ascii=False) + "\n")
    if bad:
        raise SystemExit(f"{bad} reference answer(s) do not match their SQL")
    print(f"wrote {len(CASES)} cases to {out}")


if __name__ == "__main__":
    main()
