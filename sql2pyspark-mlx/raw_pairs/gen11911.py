from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("order_stats"), F.col("orders.id") == F.col("order_stats.order_id"), "left")
    .select(
        F.col("orders.store_id"),
        F.col("orders.order_date"),
        F.col("order_stats.max_quantity"),
    )
    .orderBy(F.col("store_id").desc())
)
