from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .select(
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("t.avg_duration_sec"),
        F.col("t.event_type_values"),
    )
    .orderBy("avg_duration_sec")
)
