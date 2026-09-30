from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("subscriptions.plan").isin(["enterprise", "basic", "premium"]))
    .groupBy(F.col("customers.country"))
    .agg(F.count("*").alias("subscription_count"))
)
