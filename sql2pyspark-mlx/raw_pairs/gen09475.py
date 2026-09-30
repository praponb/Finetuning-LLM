from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", F.date_trunc("MONTH", "end_date").alias("end_month"))
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
    .orderBy(F.col("stddev_monthly_fee").desc())
)
