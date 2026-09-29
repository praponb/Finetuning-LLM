from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("event_time") >= "2025-01-15")
    .groupBy("session_id")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
)

result = (
    df_sessions.alias("sessions")
    .join(web_events_by_session.alias("web_events_by_session"), F.col("sessions.id") == F.col("web_events_by_session.session_id"), "left")
    .filter(F.col("web_events_by_session.avg_duration_sec") >= 240)
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("sessions.country"),
        F.col("web_events_by_session.avg_duration_sec"),
    )
)
