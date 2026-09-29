from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .groupBy("order_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.avg_quantity") > 60)
    .select(F.col("o.channel"), F.col("o.amount"), F.col("t.avg_quantity"))
    .orderBy(F.col("avg_quantity").desc())
)
