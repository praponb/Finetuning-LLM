from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_products"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.unique_products") > 5)
    .select(F.col("o.store_id"), F.col("ot.unique_products"), F.col("ot.avg_quantity"))
)
