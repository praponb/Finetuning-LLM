from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.count("*").alias("total_count"),
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.min("end_date").alias("first_end_date"),
    )
    .orderBy("first_end_date")
)
