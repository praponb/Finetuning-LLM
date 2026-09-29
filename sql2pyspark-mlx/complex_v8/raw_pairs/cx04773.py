from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_prev = filtered.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_quality_running))

session_summary = (
    sessionized
    .groupBy("quality", "session_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("kwh").alias("session_kwh"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

labeled = (
    session_summary
    .select(
        "*",
        F.month(F.col("session_start")).alias("session_start_month"),
        F.abs(F.col("n_events") - 4).alias("n_events_distance"),
    )
)

result = (
    labeled
    .filter(F.col("quality").isin("EST", "BAD"))
    .select(
        "quality",
        "session_id",
        "session_start",
        "session_end",
        "n_events",
        "session_kwh",
        "session_start_month",
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("session_start")).alias("days_since_session_start"),
    )
    .orderBy(
        F.col("quality").desc(),
        F.col("session_end").desc_nulls_first(),
        "session_id",
    )
    .limit(10)
)
