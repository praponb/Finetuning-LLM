from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .groupBy("order_id")
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(F.col("o.id"), F.col("o.order_date"), F.col("t.avg_unit_price"))
    .orderBy("order_date")
)
