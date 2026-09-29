from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.count("*").alias("row_count"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(web_events_by_session.alias("web_events_by_session"), F.col("sessions.id") == F.col("web_events_by_session.session_id"), "inner")
    .filter(F.col("web_events_by_session.row_count") >= 3)
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("web_events_by_session.row_count"),
        F.col("web_events_by_session.min_duration_sec"),
    )
)
