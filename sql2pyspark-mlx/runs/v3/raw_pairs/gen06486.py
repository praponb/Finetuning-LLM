from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .filter(F.col("unit_price").between(350, 400))
    .groupBy("order_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("order_stats"), F.col("orders.id") == F.col("order_stats.order_id"), "inner")
    .select(
        F.col("orders.status"),
        F.col("order_stats.unit_price_count"),
        F.col("order_stats.unique_products"),
    )
    .orderBy("unique_products")
)
