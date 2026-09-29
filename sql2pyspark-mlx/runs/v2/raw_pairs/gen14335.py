from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .select(
        F.col("s.customer_id"),
        F.col("s.country"),
        F.col("t.avg_duration_sec"),
        F.col("t.event_type_values"),
    )
)
