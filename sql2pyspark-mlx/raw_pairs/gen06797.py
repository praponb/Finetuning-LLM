from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .filter(F.col("quantity") <= 5)
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("os.stddev_unit_price"),
        F.col("os.quantity_count"),
    )
)
