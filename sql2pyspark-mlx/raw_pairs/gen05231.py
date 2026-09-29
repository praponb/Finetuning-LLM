from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") >= "2024-04-05")
    .groupBy("session_id", "event_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
    )
    .filter(F.col("add_to_cart_count") < 53)
    .select("session_id", "event_type", "page_values", "add_to_cart_count")
    .orderBy("event_type", "add_to_cart_count", "session_id")
    .limit(5)
)
