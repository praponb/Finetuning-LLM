from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
)

result = (
    df_sessions.alias("sessions")
    .join(session_summary.alias("session_summary"), F.col("sessions.id") == F.col("session_summary.session_id"), "inner")
    .select(F.col("sessions.customer_id"), F.col("session_summary.avg_duration_sec"))
    .orderBy(F.col("customer_id").desc(), F.col("avg_duration_sec").desc())
)
