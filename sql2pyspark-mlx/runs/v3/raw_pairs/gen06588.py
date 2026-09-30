from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.countDistinct("quantity").alias("unique_orders"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.unique_orders") > 7)
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("ot.unique_orders"),
        F.col("ot.unit_price_range"),
    )
)
