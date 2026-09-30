from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .groupBy("order_id")
    .agg(F.countDistinct("unit_price").alias("unique_products"))
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(F.col("o.amount"), F.col("t.unique_products"))
    .orderBy(F.col("unique_products").desc())
)
