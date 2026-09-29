from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("event_time") >= "2023-07-21")
    .groupBy("session_id")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_totals.alias("session_totals"), F.col("sessions.id") == F.col("session_totals.session_id"), "inner")
    .select(
        F.col("sessions.browser"),
        F.col("sessions.started_at"),
        F.col("session_totals.max_duration_sec"),
        F.col("session_totals.avg_duration_sec"),
    )
    .orderBy("started_at", F.col("avg_duration_sec").desc())
)
