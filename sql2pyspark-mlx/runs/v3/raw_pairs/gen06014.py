from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", F.year("start_date").alias("start_year"))
    .agg(
        F.count("*").alias("subscription_count"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
    .filter(F.col("avg_monthly_fee") > 29)
    .orderBy(F.col("start_year").desc(), "status")
    .limit(10)
)
