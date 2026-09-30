from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .filter(F.col("O.avg_unit_price") > 63)
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.status"),
        F.col("O.avg_unit_price"),
    )
    .orderBy("avg_unit_price")
)
