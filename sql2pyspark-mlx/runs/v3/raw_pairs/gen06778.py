from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.lower(F.trim("email")) == "henry@startup.io").select("id")

result = (
    df_subscriptions.alias("subscriptions")
    .join(excluded_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left_anti")
    .filter(F.col("subscriptions.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .filter(F.col("subscriptions.monthly_fee") <= 220)
    .select(F.col("subscriptions.id"), F.col("subscriptions.monthly_fee"))
)
