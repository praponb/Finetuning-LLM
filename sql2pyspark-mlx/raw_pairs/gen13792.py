from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .filter(F.col("quantity") < 6)
    .groupBy("order_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.avg_unit_price") >= 61)
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.status"),
        F.col("os.avg_quantity"),
        F.col("os.avg_unit_price"),
    )
    .orderBy(F.col("avg_unit_price").desc())
)
