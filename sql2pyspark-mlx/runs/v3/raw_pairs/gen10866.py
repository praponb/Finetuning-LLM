from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
    .filter(F.col("stddev_monthly_fee") < 21)
    .select("status", "stddev_monthly_fee", "avg_monthly_fee")
    .orderBy(F.col("stddev_monthly_fee").desc(), "status")
    .limit(50)
)
