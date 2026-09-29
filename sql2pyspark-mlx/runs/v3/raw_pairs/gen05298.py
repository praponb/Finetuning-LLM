from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(F.min("event_time").alias("min_event_time"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .select(F.col("s.id"), F.col("s.customer_id"), F.col("webs.min_event_time"))
)
