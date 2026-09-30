from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .groupBy("order_id")
    .agg(F.max("unit_price").alias("max_unit_price"))
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("order_stats"), F.col("orders.id") == F.col("order_stats.order_id"), "inner")
    .select(
        F.col("orders.id"),
        F.col("orders.customer_id"),
        F.col("order_stats.max_unit_price"),
    )
)
