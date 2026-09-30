from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("duration_sec") == 431)
    .groupBy("session_id")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_stats.alias("session_stats"), F.col("sessions.id") == F.col("session_stats.session_id"), "left")
    .filter(F.col("session_stats.avg_duration_sec") > 26)
    .select(
        F.col("sessions.id"),
        F.col("sessions.customer_id"),
        F.col("sessions.country"),
        F.col("session_stats.avg_duration_sec"),
        F.col("session_stats.event_type_values"),
    )
    .orderBy("customer_id")
)
