from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_summary.alias("SessionSummary"), F.col("sessions.id") == F.col("SessionSummary.session_id"), "inner")
    .select(
        F.col("sessions.customer_id"),
        F.col("sessions.country"),
        F.col("SessionSummary.stddev_duration_sec"),
        F.col("SessionSummary.max_duration_sec"),
    )
)
