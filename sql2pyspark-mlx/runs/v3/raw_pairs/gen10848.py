from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 232, "high").when(F.col("monthly_fee") >= 57, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0))) / F.count("*")), 1).alias("pct_premium"),
    )
    .filter(F.col("pct_premium") > 27)
    .select("monthly_fee_bucket", "pct_premium")
)
