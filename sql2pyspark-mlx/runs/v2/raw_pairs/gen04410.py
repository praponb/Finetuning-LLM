from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("session_id").isin([11, 17]))
    .groupBy("session_id")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
)

result = (
    df_sessions.alias("sessions")
    .join(web_events_by_session.alias("web_events_by_session"), F.col("sessions.id") == F.col("web_events_by_session.session_id"), "inner")
    .filter(F.col("web_events_by_session.total_duration_sec") >= 2700)
    .select(
        F.col("sessions.customer_id"),
        F.col("web_events_by_session.total_duration_sec"),
    )
)
