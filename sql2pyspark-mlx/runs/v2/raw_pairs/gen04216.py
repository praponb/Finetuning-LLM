from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .filter(F.col("order_id").isin([7, 17, 19]))
    .groupBy("order_id")
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(F.col("o.discount"), F.col("o.shipping_fee"), F.col("t.revenue"))
    .orderBy("shipping_fee")
)
