from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_subscriptions.filter(F.col("status") == "PAUSED").alias("subscriptions"), F.col("customers.id") == F.col("subscriptions.customer_id"), "left_semi")
    .filter(F.col("customers.age") < 67)
    .select(F.col("customers.id"), F.col("customers.country"), F.col("customers.email"))
)
