from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_subscriptions.alias("subscriptions"), F.col("customers.id") == F.col("subscriptions.customer_id"), "left")
    .filter(F.col("subscriptions.end_date") > "2024-12-17")
    .groupBy(F.col("customers.id"), F.col("customers.country"))
    .agg(F.count(F.col("subscriptions.id")).alias("subscription_count"))
    .orderBy(F.col("id").desc())
)
