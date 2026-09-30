from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

with_prev = filtered_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_site_id_running))

session_summary = (
    sessionized
    .groupBy("site_id", "session_no")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("kwh").alias("session_kwh"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    session_summary
    .filter(F.col("session_kwh") > 192.0)
    .select(
        "site_id",
        "session_no",
        "session_start",
        "session_end",
        "event_count",
        "session_kwh",
        "duration_min",
        F.when(F.col("event_count") >= 5, "HIGH").when(F.col("event_count") >= 3, "MEDIUM").otherwise("LOW").alias("event_count_tier"),
        F.when(F.col("duration_min") >= 156.0, "HIGH").when(F.col("duration_min") >= 48.0, "MEDIUM").otherwise("LOW").alias("duration_min_tier"),
    )
    .orderBy(F.col("duration_min_tier").desc())
)
