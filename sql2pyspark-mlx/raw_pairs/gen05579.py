from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .groupBy("order_id")
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.min("unit_price").alias("min_unit_price"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("oibo"), F.col("o.id") == F.col("oibo.order_id"), "inner")
    .filter(F.col("oibo.revenue") > 72)
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("oibo.revenue"),
        F.col("oibo.min_unit_price"),
    )
    .orderBy(F.col("shipping_fee").desc(), F.col("revenue").desc())
)
