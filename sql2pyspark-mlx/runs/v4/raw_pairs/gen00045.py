from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_count"),
        F.sum(F.when(F.col("status") == "ACTIVE", F.col("monthly_fee")).otherwise(0)).alias("active_monthly_fee"),
        F.round(((100 * F.sum(F.when(F.col("plan") == "enterprise", 1).otherwise(0))) / F.count("*")), 1).alias("pct_enterprise"),
    )
    .orderBy(F.col("paused_count").desc())
)
