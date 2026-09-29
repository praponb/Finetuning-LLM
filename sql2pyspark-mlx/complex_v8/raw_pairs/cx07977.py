from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
    )
)

with_prev = scoped_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_quality_running))

session_summary = (
    sessionized
    .groupBy("quality", "burst_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

scored = (
    session_summary
    .filter(F.col("session_start") < F.lit("2025-02-13 09:00:00").cast("timestamp"))
    .select(
        "quality",
        "burst_id",
        "session_start",
        "session_end",
        "event_count",
        F.round(F.col("duration_min") * 1.07, 2).alias("duration_min_adj"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

result = (
    scored
    .cube("quality", "event_count", "burst_id")
    .agg(
        F.grouping("quality").alias("is_total_quality"),
        F.count(F.when(F.col("quality") != "OK", 1)).alias("n_quality_ok"),
    )
    .orderBy(
        F.col("quality").asc_nulls_last(),
        F.col("event_count").asc_nulls_last(),
        F.col("burst_id").asc_nulls_last(),
    )
)
