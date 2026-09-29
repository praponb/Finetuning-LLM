from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("s.customer_id").isNotNull())
    .groupBy(F.col("c.country"))
    .agg(
        F.count(F.col("s.id")).alias("subscription_count"),
        (F.max(F.col("s.monthly_fee")) - F.min(F.col("s.monthly_fee"))).alias("monthly_fee_range"),
        F.sum(F.when(F.col("s.status") == "PAUSED", F.col("s.monthly_fee")).otherwise(0)).alias("paused_monthly_fee"),
    )
    .orderBy(F.col("monthly_fee_range").desc(), "country")
    .limit(5)
)
