from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(F.max("event_time").alias("latest_event_time"))
)

result = (
    df_sessions.alias("sessions")
    .join(web_events_by_session.alias("web_events_by_session"), F.col("sessions.id") == F.col("web_events_by_session.session_id"), "left")
    .select(
        F.col("sessions.id"),
        F.col("sessions.country"),
        F.col("sessions.device"),
        F.col("web_events_by_session.latest_event_time"),
    )
    .orderBy(F.col("id").desc())
)
