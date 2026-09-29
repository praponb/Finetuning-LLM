from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("duration_sec") == 147)
    .groupBy("session_id")
    .agg(F.max("duration_sec").alias("max_duration_sec"))
)

result = (
    df_sessions.alias("sessions")
    .join(web_events_by_session.alias("t"), F.col("sessions.id") == F.col("t.session_id"), "inner")
    .select(F.col("sessions.started_at"), F.col("t.max_duration_sec"))
    .orderBy(F.col("max_duration_sec").desc())
)
