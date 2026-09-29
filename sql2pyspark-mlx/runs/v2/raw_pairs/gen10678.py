from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .select(
        F.col("payments.id"),
        F.col("payments.paid_at"),
        F.col("orders.channel"),
        F.col("orders.amount"),
        F.col("orders.customer_id"),
    )
)
