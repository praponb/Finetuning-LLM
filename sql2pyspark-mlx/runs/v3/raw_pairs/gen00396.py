from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .filter(F.col("unit_price") <= 400)
    .groupBy("order_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.max_quantity") > 7.1)
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
        F.col("ot.max_quantity"),
    )
)
