from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([1, 7, 8]) | F.col("start_date").between("2024-05-01", "2024-09-01"))
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .filter(F.col("min_monthly_fee") >= 90)
    .drop("min_monthly_fee")
    .orderBy(F.col("start_month").desc())
)
