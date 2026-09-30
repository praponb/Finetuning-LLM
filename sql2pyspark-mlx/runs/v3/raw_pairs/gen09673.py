from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
    .filter(F.col("total_monthly_fee") > 410)
    .orderBy("status")
    .limit(10)
)
