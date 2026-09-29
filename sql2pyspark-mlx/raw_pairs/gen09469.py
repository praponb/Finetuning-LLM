from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", "plan")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0))) / F.count("*")), 1).alias("pct_premium"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
    .orderBy("avg_monthly_fee", F.col("plan").desc())
)
