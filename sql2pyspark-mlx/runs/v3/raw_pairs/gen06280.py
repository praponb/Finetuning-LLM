from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.filter(F.col("unit_price").isNull()).alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_semi")
    .filter(F.month(F.col("o.order_date")).isin([5, 6, 12]))
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
    )
)
