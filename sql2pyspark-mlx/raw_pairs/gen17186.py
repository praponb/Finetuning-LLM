from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_orders.filter(F.col("amount").between(430, 1050)).alias("orders"), F.col("customers.id") == F.col("orders.customer_id"), "left_anti")
    .select(F.col("customers.id"), F.col("customers.is_active"))
)
