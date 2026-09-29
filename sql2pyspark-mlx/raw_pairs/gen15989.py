from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .groupBy("order_id")
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.avg_unit_price") > 44)
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.amount"),
        F.col("os.avg_unit_price"),
    )
    .orderBy(F.col("channel").desc())
)
