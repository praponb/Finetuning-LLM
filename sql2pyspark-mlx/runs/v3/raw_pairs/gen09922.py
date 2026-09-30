from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .filter(F.col("quantity").between(7, 8))
    .groupBy("order_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    df_orders.alias("orders")
    .join(order_summary.alias("t"), F.col("orders.id") == F.col("t.order_id"), "left")
    .select(F.col("orders.id"), F.col("orders.status"), F.col("t.total_quantity"))
    .orderBy("status")
)
