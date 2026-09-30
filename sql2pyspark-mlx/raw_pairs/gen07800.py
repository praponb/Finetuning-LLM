from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.min("unit_price").alias("min_unit_price"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.col("ot.total_quantity"),
        F.col("ot.min_unit_price"),
    )
    .orderBy("shipping_fee")
)
