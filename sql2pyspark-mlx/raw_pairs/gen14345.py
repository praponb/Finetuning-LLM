from pyspark.sql import functions as F

recent_subscriptions = df_subscriptions.filter(F.col("start_date") <= "2025-09-01")

subscriptions_stats = (
    recent_subscriptions
    .groupBy(F.year("start_date").alias("start_year"))
    .agg(
        F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0)).alias("premium_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
    )
)

result = (
    subscriptions_stats
    .filter(F.col("premium_count") < 52)
    .select("start_year", "premium_count", "pct_cancelled")
)
