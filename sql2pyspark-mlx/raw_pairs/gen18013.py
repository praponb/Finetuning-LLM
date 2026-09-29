from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_stats.alias("session_stats"), F.col("sessions.id") == F.col("session_stats.session_id"), "left")
    .filter(F.col("session_stats.duration_sec_count") > 10)
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("sessions.customer_id"),
        F.col("session_stats.duration_sec_count"),
        F.col("session_stats.event_type_list"),
    )
)
