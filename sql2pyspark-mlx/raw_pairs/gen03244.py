from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.year("start_date").alias("start_year"),
        "plan",
        F.when(F.col("monthly_fee") >= 197, "high").when(F.col("monthly_fee") >= 82, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
)
