from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(F.count("*").alias("web_event_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.web_event_count") > 10)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.started_at"),
        F.col("t.web_event_count"),
    )
    .orderBy("started_at")
)
