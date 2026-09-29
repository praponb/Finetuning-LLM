from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.countDistinct("unit_price").alias("unique_products"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.customer_id"),
        F.col("os.stddev_quantity"),
        F.col("os.unique_products"),
    )
)
