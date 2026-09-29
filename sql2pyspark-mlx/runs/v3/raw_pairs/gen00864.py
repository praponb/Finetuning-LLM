from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("ot.min_unit_price"),
        F.col("ot.avg_quantity"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
