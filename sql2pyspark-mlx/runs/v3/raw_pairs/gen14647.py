from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
    )
    .orderBy("unit_price", F.col("id").desc())
    .limit(5)
)
