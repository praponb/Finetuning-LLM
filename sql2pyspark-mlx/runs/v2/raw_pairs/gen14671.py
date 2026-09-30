from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .filter(F.col("order_id").isin([9, 19]))
    .groupBy("order_id")
    .agg(F.avg("unit_price").alias("avg_unit_price"))
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("oibo"), F.col("o.id") == F.col("oibo.order_id"), "left")
    .filter(F.col("oibo.avg_unit_price") > 110)
    .select(F.col("o.status"), F.col("o.discount"), F.col("oibo.avg_unit_price"))
)
