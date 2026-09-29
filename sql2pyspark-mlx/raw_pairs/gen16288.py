from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter((F.col("p.paid_at") >= "2025-07-01") & (F.col("o.amount") < 1300))
    .select(
        F.col("p.method"),
        F.col("p.amount"),
        F.col("p.status"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.ceil(F.col("p.amount")).alias("amount_whole"),
    )
)
