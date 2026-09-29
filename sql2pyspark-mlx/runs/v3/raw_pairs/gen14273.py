from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.min("end_date").alias("first_end_date"),
    )
    .orderBy("first_end_date")
)
