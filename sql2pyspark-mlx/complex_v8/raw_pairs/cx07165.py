from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

with_prev = scoped_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_site_id_running))

per_session = (
    sessionized
    .groupBy("site_id", "session_no")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

annotated = (
    per_session
    .select(
        "*",
        F.date_trunc("week", F.col("session_start")).alias("session_start_week"),
        (F.col("session_end") + F.expr("INTERVAL 7 DAYS")).alias("session_end_plus_7d"),
        F.coalesce(F.col("duration_min"), F.lit(0.0)).alias("duration_min_filled"),
    )
)

result = (
    annotated
    .select(
        "site_id",
        "session_no",
        "event_count",
        "duration_min",
        "session_end_plus_7d",
        "duration_min_filled",
        (F.col("duration_min") + F.col("duration_min_filled")).alias("duration_min_plus_duration_min_filled"),
    )
    .orderBy(F.col("session_end_plus_7d").asc(), "site_id")
)
