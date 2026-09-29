from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.min("end_date").alias("min_end_date"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
    .orderBy("plan", "min_end_date")
)
