from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 160)
    .groupBy(
        F.when(F.col("monthly_fee") >= 264, "high").when(F.col("monthly_fee") >= 88, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0))) / F.count("*")), 1).alias("pct_premium"),
    )
    .filter(F.col("pct_premium") < 60)
    .orderBy("pct_premium", F.col("monthly_fee_bucket").desc())
)
