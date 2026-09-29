from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("session_id").isin([12, 20, 35, 39]))
    .groupBy("session_id")
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
)

result = (
    df_sessions.alias("sessions")
    .join(session_summary.alias("session_summary"), F.col("sessions.id") == F.col("session_summary.session_id"), "inner")
    .filter(F.col("session_summary.stddev_duration_sec") > 69)
    .select(
        F.col("sessions.id"),
        F.col("sessions.device"),
        F.col("sessions.customer_id"),
        F.col("session_summary.stddev_duration_sec"),
    )
    .orderBy(F.col("customer_id").desc())
)
