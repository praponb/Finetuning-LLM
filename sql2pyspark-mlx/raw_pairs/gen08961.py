from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("session_id").isin([12, 40]))
    .groupBy("session_id")
    .agg(F.min("event_time").alias("min_event_time"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .select(F.col("s.id"), F.col("s.browser"), F.col("ss.min_event_time"))
    .orderBy(F.col("min_event_time").desc())
)
