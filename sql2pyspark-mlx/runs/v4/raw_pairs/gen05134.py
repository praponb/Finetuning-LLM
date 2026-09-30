from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .groupBy(F.col("c.id"), F.col("c.name"))
    .agg(
        F.count(F.col("s.id")).alias("subscription_count"),
        F.round(((100 * F.sum(F.when(F.col("s.status") == "ACTIVE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_active"),
        F.min(F.col("s.monthly_fee")).alias("min_monthly_fee"),
    )
    .filter(F.col("subscription_count") > 2)
)
