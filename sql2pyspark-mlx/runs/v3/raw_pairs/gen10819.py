from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 206, "high").when(F.col("monthly_fee") >= 46, "mid").otherwise("low").alias("monthly_fee_bucket"),
        "status",
    )
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
    .filter(F.col("min_monthly_fee") < 150)
)
