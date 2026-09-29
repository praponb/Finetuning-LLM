from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "outer")
    .filter(F.col("customers.signup_date") <= "2023-12-01")
    .select(
        F.col("payments.id"),
        F.col("payments.paid_at"),
        F.col("orders.customer_id"),
        F.col("orders.order_date"),
        F.col("customers.email"),
    )
)
