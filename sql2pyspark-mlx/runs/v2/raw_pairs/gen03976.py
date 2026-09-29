from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "outer")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter((F.col("p.paid_at") >= "2025-05-28") & F.col("p.order_id").isNull())
    .select(
        F.col("p.id"),
        F.col("p.amount"),
        F.col("o.status"),
        F.col("o.shipping_fee"),
        F.col("s.sqft"),
    )
)
