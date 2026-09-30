from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.count("*").alias("total_count"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_stats.alias("session_stats"), F.col("sessions.id") == F.col("session_stats.session_id"), "left")
    .filter(F.col("session_stats.total_count") > 6)
    .select(
        F.col("sessions.id"),
        F.col("sessions.customer_id"),
        F.col("session_stats.total_count"),
        F.col("session_stats.stddev_duration_sec"),
    )
    .orderBy(F.col("customer_id").desc())
)
