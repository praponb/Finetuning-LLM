from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.min("unit_price").alias("min_unit_price"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.stddev_unit_price") > 75)
    .select(
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("ot.stddev_unit_price"),
        F.col("ot.min_unit_price"),
    )
)
