from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("duration_sec") == 413)
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("t.event_type_values"),
        F.col("t.total_count"),
    )
    .orderBy(F.col("event_type_values").desc())
)
