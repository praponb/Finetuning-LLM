from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.customer_id") == 32)
    .groupBy(F.col("c.city"))
    .agg(
        F.min(F.col("s.monthly_fee")).alias("min_monthly_fee"),
        F.round(((100 * F.sum(F.when(F.col("s.status") == "PAUSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paused"),
    )
)
