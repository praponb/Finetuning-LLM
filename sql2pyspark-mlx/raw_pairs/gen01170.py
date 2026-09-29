from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("p.order_id") == 14)
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("p.paid_at"),
        F.col("o.discount"),
        F.col("o.amount"),
        F.col("o.shipping_fee"),
    )
)
