from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .filter(F.col("quantity") >= 8)
    .groupBy("order_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
        F.col("os.avg_unit_price"),
        F.col("os.min_quantity"),
    )
)
