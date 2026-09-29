from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_event_count_by_sessi = Window.partitionBy("quality", "event_count").orderBy("session_end", "quality", "session_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_event_count_by_sessi_2 = Window.partitionBy("quality", "event_count").orderBy("session_end", "quality", "session_id")

recent_readings = (
    df_meter_readings
    .filter(F.col("kwh") >= 42.1)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("voltage") >= 235.0, "large").when(F.col("voltage") >= 222.5, "medium").otherwise("small").alias("voltage_tier"),
    )
)

with_prev = recent_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_quality_running))

sessions = (
    sessionized
    .groupBy("quality", "session_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

qualified = (
    sessions
    .crossJoin(sessions.agg(F.max("duration_min").alias("overall_max_duration_min")))
    .filter(F.col("duration_min") < F.col("overall_max_duration_min"))
    .drop("overall_max_duration_min")
)

with_windows = (
    qualified
    .withColumn("last_duration_min", F.last("duration_min").over(w_quality_event_count_by_sessi))
    .withColumn("session_end_percent_rank", F.round(F.percent_rank().over(w_quality_event_count_by_sessi_2), 3))
)

result = (
    with_windows
    .select(
        "quality",
        "session_end",
        "event_count",
        "duration_min",
        "session_end_percent_rank",
    )
)
