from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("payments.amount").between(580, 810))
    .select(
        F.col("payments.method"),
        F.col("orders.discount"),
        F.col("customers.country"),
    )
)
