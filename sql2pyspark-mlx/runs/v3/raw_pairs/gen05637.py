from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .select(
        F.col("p.method"),
        F.col("p.paid_at"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
    )
)
