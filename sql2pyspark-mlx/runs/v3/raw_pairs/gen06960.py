from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
)
