from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .select(
        F.col("payments.id"),
        F.col("payments.paid_at"),
        F.col("payments.status"),
        F.col("orders.amount"),
        F.col("orders.discount"),
        F.col("customers.city"),
        F.col("customers.segment"),
    )
)
