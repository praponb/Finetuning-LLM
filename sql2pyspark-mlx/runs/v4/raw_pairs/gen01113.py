from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("event_time") >= "2024-03-02")
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
        F.min("event_time").alias("min_event_time"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(web_events_by_session.alias("WebEventsBySession"), F.col("sessions.id") == F.col("WebEventsBySession.session_id"), "left")
    .select(
        F.col("sessions.id"),
        F.col("sessions.country"),
        F.col("sessions.device"),
        F.col("WebEventsBySession.page_view_count"),
        F.col("WebEventsBySession.min_event_time"),
    )
)
