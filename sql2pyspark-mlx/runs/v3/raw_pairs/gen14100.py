from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([20, 28, 37, 40]))
    .groupBy("channel", "status")
    .agg(
        F.count("*").alias("num_orders"),
        F.min("order_date").alias("min_order_date"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .orderBy("status", "channel")
    .limit(1)
)
