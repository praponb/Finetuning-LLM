from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_subscriptions.alias("subscriptions"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.age") > 32)
    .groupBy(F.col("customers.country"))
    .agg(
        F.count(F.col("subscriptions.id")).alias("subscription_count"),
        F.sum(F.col("subscriptions.monthly_fee")).alias("total_monthly_fee"),
    )
    .orderBy("country", F.col("subscription_count").desc())
)
