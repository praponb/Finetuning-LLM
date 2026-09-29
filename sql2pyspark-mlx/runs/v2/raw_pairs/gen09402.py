from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "outer")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .select(
        F.col("payments.amount"),
        F.col("orders.order_date"),
        F.col("orders.status"),
        F.col("customers.city"),
        F.col("customers.credit_limit"),
        F.upper(F.trim(F.col("customers.email"))).alias("email_normalized"),
    )
)
