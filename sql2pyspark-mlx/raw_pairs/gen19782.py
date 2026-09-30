from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .filter(F.col("quantity") > 3)
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("O"), F.col("o.id") == F.col("O.order_id"), "left")
    .filter(F.col("O.stddev_unit_price") >= 40)
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.store_id"),
        F.col("O.stddev_unit_price"),
        F.col("O.avg_quantity"),
    )
    .orderBy(F.col("stddev_unit_price").desc())
)
