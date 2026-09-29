from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .filter((F.col("orders.amount") < 730) & (F.col("payments.status") == "FAILED"))
    .select(
        F.col("payments.id"),
        F.col("payments.status"),
        F.col("payments.amount"),
        F.col("payments.paid_at"),
        F.col("orders.status").alias("order_status"),
    )
)
