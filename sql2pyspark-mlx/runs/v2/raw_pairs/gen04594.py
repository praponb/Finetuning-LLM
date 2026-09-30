from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.quarter("start_date").alias("start_quarter"))
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.max("start_date").alias("last_start_date"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
)
