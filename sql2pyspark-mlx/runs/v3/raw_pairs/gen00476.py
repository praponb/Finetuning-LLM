from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .filter(F.col("quantity").between(7, 8))
    .groupBy("order_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("t"), F.col("orders.id") == F.col("t.order_id"), "left")
    .select(
        F.col("orders.id"),
        F.col("orders.channel"),
        F.col("orders.store_id"),
        F.col("t.total_quantity"),
    )
    .orderBy(F.col("store_id").desc())
)
