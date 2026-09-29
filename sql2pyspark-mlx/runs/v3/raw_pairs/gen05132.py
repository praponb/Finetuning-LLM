from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.min("end_date").alias("min_end_date"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
    .filter(F.col("avg_monthly_fee") > 20)
)
