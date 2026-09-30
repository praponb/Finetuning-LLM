from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("start_date") == 2025)
    .groupBy(
        F.month("end_date").alias("end_month"),
        "plan",
        F.when(F.col("monthly_fee") >= 229, "high").when(F.col("monthly_fee") >= 80, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.min("start_date").alias("min_start_date"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
)
