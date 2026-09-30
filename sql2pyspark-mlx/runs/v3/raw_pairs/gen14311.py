from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .groupBy("order_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .filter(F.col("O.quantity_count") >= 6)
    .select(F.col("o.discount"), F.col("o.channel"), F.col("O.quantity_count"))
)
