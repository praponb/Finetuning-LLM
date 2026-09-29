from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "right")
    .filter(F.col("oi.order_id") == 35)
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("oi.product_id"),
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
        F.col("o.status"),
        F.round(F.col("o.discount"), 2).alias("discount_rounded"),
    )
    .orderBy(F.col("product_id").desc())
)
