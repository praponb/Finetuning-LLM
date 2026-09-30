from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.hour("event_time").between(12, 16))
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .select(
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("ss.page_values"),
        F.col("ss.total_duration_sec"),
    )
    .orderBy("page_values")
)
