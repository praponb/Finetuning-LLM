from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.min("event_time").alias("min_event_time"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(web_events_by_session.alias("web_events_by_session"), F.col("sessions.id") == F.col("web_events_by_session.session_id"), "inner")
    .filter(F.col("web_events_by_session.max_duration_sec") >= 140)
    .select(
        F.col("sessions.country"),
        F.col("web_events_by_session.max_duration_sec"),
        F.col("web_events_by_session.min_event_time"),
    )
)
