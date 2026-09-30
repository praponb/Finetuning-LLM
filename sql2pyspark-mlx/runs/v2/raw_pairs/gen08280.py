from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("payments.order_id"), "inner")
    .filter(F.to_date(F.col("payments.paid_at")) < "2023-10-04")
    .select(
        F.col("payments.id"),
        F.col("payments.amount"),
        F.col("payments.status"),
        F.col("payments.paid_at"),
        F.col("orders.status").alias("order_status"),
        F.col("orders.amount").alias("order_amount"),
        F.col("orders.shipping_fee"),
    )
    .orderBy(F.col("paid_at").desc())
)
