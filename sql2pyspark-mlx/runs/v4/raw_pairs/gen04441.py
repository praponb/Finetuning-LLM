from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.to_date("event_time") < "2023-05-17")
    .groupBy("session_id")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)

result = (
    df_sessions.alias("sessions")
    .join(session_summary.alias("session_summary"), F.col("sessions.id") == F.col("session_summary.session_id"), "inner")
    .select(
        F.col("sessions.id"),
        F.col("sessions.country"),
        F.col("session_summary.min_duration_sec"),
    )
    .orderBy("country", F.col("min_duration_sec").desc())
)
