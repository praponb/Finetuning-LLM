from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.max("start_date").alias("last_start_date"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
)
