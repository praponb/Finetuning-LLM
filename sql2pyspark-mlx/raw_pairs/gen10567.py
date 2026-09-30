from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.hour(F.col("p.paid_at")).between(10, 14))
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("p.paid_at"),
        F.col("o.shipping_fee"),
    )
    .orderBy(F.col("method").desc(), "id")
    .limit(3)
)
