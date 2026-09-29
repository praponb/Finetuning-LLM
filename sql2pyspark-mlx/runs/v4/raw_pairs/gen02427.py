from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.sum(F.when(F.col("page") == "/product", F.col("duration_sec")).otherwise(0)).alias("product_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.product_duration_sec") >= 27)
    .select(
        F.col("s.country"),
        F.col("s.customer_id"),
        F.col("ss.event_type_values"),
        F.col("ss.product_duration_sec"),
    )
)
