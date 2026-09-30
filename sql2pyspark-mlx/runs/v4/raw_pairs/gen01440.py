from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .filter(F.col("quantity").between(3, 4))
    .groupBy("order_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.quantity_count") > 4)
    .select(
        F.col("o.amount"),
        F.col("o.discount"),
        F.col("os.quantity_count"),
        F.col("os.total_unit_price"),
    )
    .orderBy("quantity_count")
)
