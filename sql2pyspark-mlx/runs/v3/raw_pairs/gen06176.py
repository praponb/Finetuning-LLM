from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .groupBy("order_id")
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("oibo"), F.col("o.id") == F.col("oibo.order_id"), "inner")
    .filter(F.col("oibo.revenue") >= 26)
    .select(F.col("o.id"), F.col("o.amount"), F.col("oibo.revenue"))
    .orderBy("revenue")
)
