from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        "customer_id",
        F.when(F.col("monthly_fee") >= 220, "high").when(F.col("monthly_fee") >= 47, "mid").otherwise("low").alias("monthly_fee_bucket"),
        F.when(F.col("status").isin(["CANCELLED", "PAUSED"]), "tier_1").otherwise("other").alias("status_group"),
    )
    .agg(F.avg("monthly_fee").alias("avg_monthly_fee"))
    .filter(F.col("avg_monthly_fee") >= 150)
    .orderBy("avg_monthly_fee", "monthly_fee_bucket")
)
