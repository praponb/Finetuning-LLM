from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(F.min("event_time").alias("first_event_time"))
)

result = (
    df_sessions.alias("sessions")
    .join(web_events_by_session.alias("t"), F.col("sessions.id") == F.col("t.session_id"), "inner")
    .select(F.col("sessions.customer_id"), F.col("t.first_event_time"))
    .orderBy("customer_id")
)
