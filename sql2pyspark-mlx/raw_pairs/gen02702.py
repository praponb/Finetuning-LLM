from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.filter(F.col("signup_date") <= "2023-03-25").alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left_semi")
    .filter(F.col("orders.discount") <= 0.28)
    .select(F.col("orders.id"), F.col("orders.customer_id"))
)
