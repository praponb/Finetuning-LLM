from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.col("c.id"), F.col("c.city"))
    .agg(
        F.count(F.col("s.id")).alias("subscription_count"),
        F.min(F.col("s.monthly_fee")).alias("min_monthly_fee"),
        F.sum(F.col("s.monthly_fee")).alias("total_monthly_fee"),
    )
    .filter(F.col("subscription_count") > 4)
    .orderBy(F.col("total_monthly_fee").desc())
)
