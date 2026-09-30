from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.date_format("end_date", "yyyy-MM").alias("end_month"),
        F.year("start_date").alias("start_year"),
    )
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
)
