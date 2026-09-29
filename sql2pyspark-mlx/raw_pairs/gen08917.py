from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.month("event_time").alias("event_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_count"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
    )
    .orderBy(F.col("checkout_count").desc(), F.col("event_month").desc())
    .limit(10)
)
