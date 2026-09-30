from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .filter(F.col("order_id").isin([22, 28, 32, 34]))
    .groupBy("order_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.store_id"),
        F.col("os.max_quantity"),
    )
)
