from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.sum(F.when(F.col("page") == "/product", 1).otherwise(0)).alias("product_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.product_count") > 53)
    .select(
        F.col("s.customer_id"),
        F.col("t.event_type_list"),
        F.col("t.product_count"),
    )
    .orderBy(F.col("product_count").desc())
)
