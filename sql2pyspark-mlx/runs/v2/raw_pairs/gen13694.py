from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(F.countDistinct("quantity").alias("unique_orders"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.amount"),
        F.col("os.unique_orders"),
    )
)
