from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_orders.alias("orders"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("orders.order_date") >= "2023-07-18")
    .select(
        F.col("customers.email"),
        F.col("customers.segment"),
        F.col("customers.signup_date"),
        F.col("orders.status"),
    )
)
