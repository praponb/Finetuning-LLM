from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .filter((F.col("p.paid_at") >= "2024-11-12") & (F.col("o.shipping_fee") > 30))
    .select(
        F.col("p.status"),
        F.col("p.amount"),
        F.col("o.status").alias("order_status"),
        F.col("o.shipping_fee"),
        F.substring(F.col("p.status"), 1, 2).alias("status_prefix"),
    )
    .orderBy(F.col("status_prefix").desc())
)
