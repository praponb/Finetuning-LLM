from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("oi.quantity").between(3, 5) | F.col("o.shipping_fee").between(19, 33))
    .select(
        F.col("oi.order_id"),
        F.col("o.shipping_fee"),
        F.col("o.status"),
        F.round(F.col("o.discount"), 1).alias("discount_rounded"),
    )
)
