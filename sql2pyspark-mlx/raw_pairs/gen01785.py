from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.countDistinct("duration_sec").alias("unique_pages"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_totals.alias("session_totals"), F.col("sessions.id") == F.col("session_totals.session_id"), "left")
    .filter(F.col("session_totals.stddev_duration_sec") > 44)
    .select(
        F.col("sessions.id"),
        F.col("sessions.browser"),
        F.col("sessions.started_at"),
        F.col("session_totals.unique_pages"),
        F.col("session_totals.stddev_duration_sec"),
    )
)
