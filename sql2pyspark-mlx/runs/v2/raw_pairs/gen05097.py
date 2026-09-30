from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .select(
        F.col("oi.order_id"),
        F.col("oi.product_id"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.col("o.customer_id"),
        F.round(F.col("o.discount"), 2).alias("discount_rounded"),
    )
)
