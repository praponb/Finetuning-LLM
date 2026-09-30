from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(F.count("*").alias("web_event_count"))
)

result = (
    df_sessions.alias("sessions")
    .join(web_events_by_session.alias("web_events_by_session"), F.col("sessions.id") == F.col("web_events_by_session.session_id"), "inner")
    .select(
        F.col("sessions.id"),
        F.col("sessions.device"),
        F.col("web_events_by_session.web_event_count"),
    )
    .orderBy(F.col("id").desc(), F.col("web_event_count").desc())
    .limit(50)
)
