from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .filter(F.col("status") == "DELIVERED")
    .groupBy("order_id")
    .agg(
        F.countDistinct("cost").alias("unique_orders"),
        F.sum(F.when(F.col("status") == "LOST", 1).otherwise(0)).alias("lost_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.lost_count") >= 63)
    .select(F.col("o.shipping_fee"), F.col("os.unique_orders"), F.col("os.lost_count"))
)
