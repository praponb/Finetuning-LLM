from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .groupBy("order_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("oibo"), F.col("o.id") == F.col("oibo.order_id"), "left")
    .select(F.col("o.store_id"), F.col("o.status"), F.col("oibo.max_quantity"))
    .orderBy(F.col("max_quantity").desc(), "status")
)
