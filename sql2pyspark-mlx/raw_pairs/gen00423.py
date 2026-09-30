from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .filter(F.col("order_id").isin([5, 26, 39]))
    .groupBy("order_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("oibo"), F.col("o.id") == F.col("oibo.order_id"), "inner")
    .filter(F.col("oibo.revenue") > 39)
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.status"),
        F.col("oibo.total_unit_price"),
        F.col("oibo.revenue"),
    )
    .orderBy(F.col("revenue").desc())
)
