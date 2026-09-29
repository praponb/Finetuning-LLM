from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_trunc("MONTH", "end_date").alias("end_month"))
    .agg(
        F.min("start_date").alias("min_start_date"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
    .orderBy(F.col("end_month").desc())
)
