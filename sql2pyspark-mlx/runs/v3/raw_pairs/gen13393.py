from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.year("start_date").alias("start_year"))
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
)
