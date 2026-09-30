from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(F.count("duration_sec").alias("duration_sec_count"))
)

result = (
    df_sessions.alias("sessions")
    .join(web_events_by_session.alias("web_events_by_session"), F.col("sessions.id") == F.col("web_events_by_session.session_id"), "inner")
    .filter(F.col("web_events_by_session.duration_sec_count") > 5)
    .select(
        F.col("sessions.country"),
        F.col("sessions.customer_id"),
        F.col("web_events_by_session.duration_sec_count"),
    )
)
