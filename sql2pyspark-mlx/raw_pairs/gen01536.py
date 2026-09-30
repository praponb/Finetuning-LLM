from pyspark.sql import functions as F

filtered_web_events = df_web_events.filter(F.col("session_id").isin([25, 26]))

web_events_stats = (
    filtered_web_events
    .groupBy(
        F.quarter("event_time").alias("event_quarter"),
        F.when(F.col("duration_sec") >= 387, "high").when(F.col("duration_sec") >= 193, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
)

result = (
    web_events_stats
    .select("event_quarter", "duration_sec_bucket", "cart_count", "event_type_values")
)
