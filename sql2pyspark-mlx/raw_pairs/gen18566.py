from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .select(
        F.col("payments.id"),
        F.col("payments.method"),
        F.col("payments.amount"),
        F.col("orders.status"),
        F.col("orders.channel"),
        F.col("customers.age"),
    )
)
