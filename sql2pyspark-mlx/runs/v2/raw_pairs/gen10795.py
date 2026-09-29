from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "outer")
    .select(
        F.col("payments.method"),
        F.col("payments.status"),
        F.col("payments.paid_at"),
        F.col("orders.amount"),
        F.col("orders.store_id"),
    )
)
