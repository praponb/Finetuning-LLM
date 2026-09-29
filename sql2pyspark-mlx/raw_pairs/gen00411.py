from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(
        F.col("s.started_at"),
        F.col("t.stddev_duration_sec"),
        F.col("t.total_count"),
    )
    .orderBy("stddev_duration_sec", "total_count")
)
