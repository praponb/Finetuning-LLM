from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 203, "high").when(F.col("monthly_fee") >= 68, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
    .filter(F.col("stddev_monthly_fee") > 47)
    .orderBy(F.col("stddev_monthly_fee").desc(), "monthly_fee_bucket")
    .limit(5)
)
