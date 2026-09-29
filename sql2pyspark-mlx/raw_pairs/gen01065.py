from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 100)
    .groupBy(
        F.when(F.col("monthly_fee") >= 217, "high").when(F.col("monthly_fee") >= 58, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.sum(F.when(F.col("plan") == "basic", F.col("monthly_fee")).otherwise(0)).alias("basic_monthly_fee"),
    )
)
