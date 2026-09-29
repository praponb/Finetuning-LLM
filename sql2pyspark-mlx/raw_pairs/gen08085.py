from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .filter(F.col("quantity").isNotNull())
    .groupBy("order_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.min("unit_price").alias("min_unit_price"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.total_unit_price") > 1800)
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.amount"),
        F.col("os.total_unit_price"),
        F.col("os.min_unit_price"),
    )
)
