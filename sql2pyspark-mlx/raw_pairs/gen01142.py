from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .filter(F.col("quantity").isNotNull())
    .groupBy("order_id")
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("t"), F.col("orders.id") == F.col("t.order_id"), "inner")
    .select(F.col("orders.amount"), F.col("t.revenue"), F.col("t.unique_products"))
)
