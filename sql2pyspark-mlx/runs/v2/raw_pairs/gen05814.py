from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page", F.month("event_time").alias("event_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0)).alias("cart_count"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
)
