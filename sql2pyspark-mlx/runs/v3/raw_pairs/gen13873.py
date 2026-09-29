from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .filter(F.col("quantity").between(4, 6))
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.status"),
        F.col("os.stddev_unit_price"),
        F.col("os.row_count"),
    )
    .orderBy("row_count")
)
