from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(F.min("event_time").alias("first_event_time"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("W"), F.col("s.id") == F.col("W.session_id"), "inner")
    .select(F.col("s.customer_id"), F.col("W.first_event_time"))
    .orderBy(F.col("customer_id").desc())
)
