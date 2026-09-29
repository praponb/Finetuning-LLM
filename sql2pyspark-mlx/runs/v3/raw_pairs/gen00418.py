from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .filter(F.col("quantity") != 6)
    .groupBy("order_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.stddev_quantity") > 77)
    .select(F.col("o.status"), F.col("o.amount"), F.col("ot.stddev_quantity"))
    .orderBy(F.col("status").desc())
)
