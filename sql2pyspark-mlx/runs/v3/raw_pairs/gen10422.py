from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.min("start_date").alias("first_start_date"),
    )
)
