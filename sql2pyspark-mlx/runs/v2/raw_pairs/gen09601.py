from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "right")
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("p.method"),
        F.col("p.amount"),
        F.col("o.shipping_fee"),
        F.col("o.discount"),
    )
    .orderBy(F.col("discount").desc())
)
