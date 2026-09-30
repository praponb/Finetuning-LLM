from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(web_events_by_session.alias("web_events_by_session"), F.col("sessions.id") == F.col("web_events_by_session.session_id"), "left")
    .select(
        F.col("sessions.customer_id"),
        F.col("sessions.started_at"),
        F.col("web_events_by_session.total_duration_sec"),
        F.col("web_events_by_session.duration_sec_count"),
    )
)
