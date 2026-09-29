from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(F.min("event_time").alias("min_event_time"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("ss.min_event_time"),
    )
)
