from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .select(
        F.col("p.order_id"),
        F.col("o.status"),
        F.col("o.amount"),
        F.col("s.store_name"),
        F.least(F.col("o.discount"), F.col("o.shipping_fee")).alias("least_discount_shipping_fee"),
    )
)
