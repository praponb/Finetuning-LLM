from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.count("*").alias("num_web_events"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.avg_duration_sec") >= 420)
    .select(
        F.col("s.started_at"),
        F.col("t.avg_duration_sec"),
        F.col("t.num_web_events"),
    )
)
