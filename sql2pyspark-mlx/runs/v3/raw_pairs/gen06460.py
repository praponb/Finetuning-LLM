from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .filter(F.col("unit_price") < 300)
    .groupBy("order_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.total_unit_price") >= 800)
    .select(
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
        F.col("ot.total_unit_price"),
        F.col("ot.min_quantity"),
    )
)
