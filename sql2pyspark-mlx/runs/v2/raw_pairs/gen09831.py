from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)

result = (
    df_sessions.alias("sessions")
    .join(web_events_by_session.alias("t"), F.col("sessions.id") == F.col("t.session_id"), "left")
    .select(
        F.col("sessions.device"),
        F.col("sessions.country"),
        F.col("t.min_duration_sec"),
    )
    .orderBy("device")
)
