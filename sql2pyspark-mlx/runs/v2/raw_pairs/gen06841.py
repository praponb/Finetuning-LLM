from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .filter(F.col("subscriptions.customer_id").isNotNull())
    .join(df_customers.filter(F.lower(F.trim("email")) == "henry@startup.io").alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left_anti")
    .filter(F.col("subscriptions.monthly_fee") <= 220)
    .select(F.col("subscriptions.id"), F.col("subscriptions.monthly_fee"))
)
