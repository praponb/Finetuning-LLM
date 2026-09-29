from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 110)
    .groupBy("plan", "status")
    .agg(
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_count"),
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee")).otherwise(0)).alias("premium_monthly_fee"),
    )
    .orderBy("status")
)
