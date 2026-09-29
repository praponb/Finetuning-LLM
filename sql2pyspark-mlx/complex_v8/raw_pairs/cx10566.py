from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_or_unknown = Window.partitionBy("quality_or_unknown").orderBy("read_at", "id")
w_quality_or_unknown_running = Window.partitionBy("quality_or_unknown").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

with_prev = recent_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality_or_unknown))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_quality_or_unknown_running))

per_session = (
    sessionized
    .groupBy("quality_or_unknown", "session_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("kwh").alias("session_kwh"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

screened = (
    per_session
    .crossJoin(per_session.agg(F.avg("duration_min").alias("overall_avg_duration_min")))
    .filter(F.col("duration_min") > F.col("overall_avg_duration_min"))
    .drop("overall_avg_duration_min")
)

with_activity = (
    screened
    .crossJoin(screened.agg(F.avg("session_id").alias("overall_avg_session_id")))
    .filter(F.col("session_id") > F.col("overall_avg_session_id"))
    .drop("overall_avg_session_id")
)

result = (
    with_activity
    .rollup("events", "quality_or_unknown", "session_id")
    .agg(
        F.countDistinct("quality_or_unknown").alias("distinct_quality_or_unknowns"),
    )
    .orderBy(
        F.col("events").asc_nulls_last(),
        F.col("quality_or_unknown").asc_nulls_last(),
        F.col("session_id").asc_nulls_last(),
    )
)
