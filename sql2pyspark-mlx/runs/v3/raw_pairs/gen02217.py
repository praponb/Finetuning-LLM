from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        "plan",
        F.when(F.col("monthly_fee") >= 262, "high").when(F.col("monthly_fee") >= 58, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.avg("monthly_fee").alias("avg_monthly_fee"))
    .filter(F.col("avg_monthly_fee") > 100)
)
