from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["standard", "enterprise", "premium"]) & (F.col("monthly_fee") >= 200))
    .groupBy(
        F.when(F.col("monthly_fee") >= 221, "high").when(F.col("monthly_fee") >= 71, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.avg("monthly_fee").alias("avg_monthly_fee"))
    .orderBy("monthly_fee_bucket")
)
