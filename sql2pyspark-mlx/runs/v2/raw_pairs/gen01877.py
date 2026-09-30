from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("event_type") == "click")
    .groupBy("session_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_totals.alias("session_totals"), F.col("sessions.id") == F.col("session_totals.session_id"), "inner")
    .filter(F.col("session_totals.avg_duration_sec") > 180)
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("sessions.country"),
        F.col("session_totals.total_duration_sec"),
        F.col("session_totals.avg_duration_sec"),
    )
)
