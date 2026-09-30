from pyspark.sql import functions as F

src = (
    df_orders
    .filter(F.col("store_id") == 2)
    .withColumn("order_date_month_end", F.last_day("order_date"))
)

orders_stats = (
    src
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    orders_stats
    .select("status", "channel_values", "status_list")
    .orderBy("channel_values")
)
