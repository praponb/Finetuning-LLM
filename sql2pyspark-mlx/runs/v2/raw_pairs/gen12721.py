from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "right")
    .filter(F.col("payments.method").isin(["paypal", "card"]))
    .select(
        F.col("payments.amount"),
        F.col("payments.paid_at"),
        F.col("payments.status"),
        F.col("orders.shipping_fee"),
        F.col("orders.status").alias("order_status"),
        F.col("orders.store_id"),
        F.substring(F.col("payments.status"), 1, 2).alias("status_prefix"),
    )
    .orderBy("paid_at")
)
