from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .groupBy("order_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("oibo"), F.col("o.id") == F.col("oibo.order_id"), "inner")
    .filter(F.col("oibo.total_quantity") >= 25)
    .select(F.col("o.id"), F.col("o.channel"), F.col("oibo.total_quantity"))
    .orderBy(F.col("id").desc())
)
