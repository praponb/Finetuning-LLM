from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("subscriptions.customer_id"), "inner")
    .filter(F.col("subscriptions.status").isin(["PAUSED", "CANCELLED", "EXPIRED"]))
    .groupBy(F.col("customers.country"))
    .agg(
        F.sum(F.when(F.col("subscriptions.plan") == "enterprise", 1).otherwise(0)).alias("enterprise_count"),
        F.count(F.when(F.col("subscriptions.plan") == "premium", 1)).alias("premium_count"),
    )
    .orderBy("enterprise_count")
)
