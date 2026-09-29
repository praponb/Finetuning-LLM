from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.paid_at") >= "2024-03-01")
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("p.method"),
        F.col("p.amount"),
        F.col("o.shipping_fee"),
    )
)
