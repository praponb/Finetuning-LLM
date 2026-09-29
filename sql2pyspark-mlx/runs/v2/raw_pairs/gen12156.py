from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_subscriptions.filter(F.col("end_date").between("2023-11-01", "2023-11-20")).alias("subscriptions"), F.col("customers.id") == F.col("subscriptions.customer_id"), "left_anti")
    .select(F.col("customers.id"), F.col("customers.is_active"))
)
