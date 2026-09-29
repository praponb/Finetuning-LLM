from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("payments.order_id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .select(
        F.col("payments.method"),
        F.col("payments.paid_at"),
        F.col("orders.customer_id"),
        F.col("customers.name"),
    )
)
