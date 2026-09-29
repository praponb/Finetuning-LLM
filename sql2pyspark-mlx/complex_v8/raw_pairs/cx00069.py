from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-01-19 12:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_prev = valid.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_quality_running))

session_summary = (
    sessionized
    .groupBy("quality", "burst_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("voltage").alias("session_voltage"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

screened = (
    session_summary
    .crossJoin(session_summary.agg(F.max("burst_id").alias("overall_max_burst_id")))
    .filter(F.col("burst_id") < F.col("overall_max_burst_id"))
    .drop("overall_max_burst_id")
)

result = (
    screened
    .select(
        "quality",
        "burst_id",
        "session_start",
        "session_end",
        "event_count",
        "duration_min",
        F.hour(F.col("session_start")).alias("session_start_hour"),
        F.round(F.col("burst_id") / F.nullif(F.col("duration_min"), F.lit(0)), 3).alias("burst_id_per_duration_min"),
    )
    .orderBy(
        F.col("event_count").asc_nulls_last(),
        F.col("burst_id").asc_nulls_last(),
    )
)
