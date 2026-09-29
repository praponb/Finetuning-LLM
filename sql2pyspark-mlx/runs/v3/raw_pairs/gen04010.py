from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("p.method"),
        F.col("o.status"),
        F.col("o.shipping_fee"),
        F.col("s.sqft"),
        (F.col("o.discount") - F.col("o.shipping_fee")).alias("discount_minus_shipping_fee"),
    )
    .orderBy("id")
    .limit(1)
)
