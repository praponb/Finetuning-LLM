from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.year("start_date").alias("start_year"),
        F.month("start_date").alias("start_month"),
    )
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .filter(F.col("min_monthly_fee") > 70)
    .select("start_year", "start_month", "avg_monthly_fee", "min_monthly_fee")
    .orderBy(F.col("start_year").desc())
)
