from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("page") == "/cart")
    .groupBy("session_id")
    .agg(
        F.count("*").alias("cnt"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(web_events_by_session.alias("web_events_by_session"), F.col("sessions.id") == F.col("web_events_by_session.session_id"), "left")
    .filter(F.col("web_events_by_session.total_duration_sec") >= 2500)
    .select(
        F.col("sessions.id"),
        F.col("sessions.browser"),
        F.col("sessions.country"),
        F.col("web_events_by_session.cnt"),
        F.col("web_events_by_session.total_duration_sec"),
    )
)
