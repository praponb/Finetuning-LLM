from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(F.min("event_time").alias("first_event_time"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .select(F.col("s.browser"), F.col("t.first_event_time"))
    .orderBy("browser", "first_event_time")
)
