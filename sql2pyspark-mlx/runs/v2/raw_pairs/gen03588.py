from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_subscriptions.filter(F.col("start_date").between("2023-07-20", "2024-12-26")).alias("subscriptions"), F.col("customers.id") == F.col("subscriptions.customer_id"), "left_anti")
    .select(F.col("customers.id"), F.col("customers.country"), F.col("customers.name"))
)
