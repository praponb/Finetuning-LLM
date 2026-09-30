from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .select(
        F.col("payments.amount"),
        F.col("payments.paid_at"),
        F.col("payments.status"),
        F.col("orders.order_date"),
        F.col("orders.customer_id"),
    )
)
