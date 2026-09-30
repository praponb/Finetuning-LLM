from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .filter(F.col("quantity").between(5, 6))
    .groupBy("order_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.max("unit_price").alias("max_unit_price"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.max_unit_price") > 310)
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("os.avg_quantity"),
        F.col("os.max_unit_price"),
    )
    .orderBy("avg_quantity", F.col("max_unit_price").desc())
)
