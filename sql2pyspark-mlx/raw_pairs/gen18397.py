from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("start_date") >= 2024)
    .groupBy(
        "plan",
        F.when(F.col("monthly_fee") >= 261, "high").when(F.col("monthly_fee") >= 48, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.min("start_date").alias("first_start_date"),
    )
    .orderBy("monthly_fee_bucket", "plan")
    .limit(20)
)
