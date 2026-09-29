from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.alias("payments"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .select(
        F.col("orders.order_date"),
        F.col("payments.paid_at"),
        F.lower(F.col("payments.status")).alias("status_lower"),
    )
)
