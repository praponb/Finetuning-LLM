from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .filter(F.col("unit_price") <= 270)
    .groupBy("order_id")
    .agg(F.countDistinct("unit_price").alias("unique_orders"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .filter(F.col("O.unique_orders") > 9)
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.order_date"),
        F.col("O.unique_orders"),
    )
    .orderBy("status")
)
