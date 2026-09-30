from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.year("end_date") < 2024, "before_2024").when(F.year("end_date") == 2024, "2024").otherwise("after_2024").alias("end_cohort"),
        F.when(F.col("monthly_fee") >= 222, "high").when(F.col("monthly_fee") >= 59, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
)
