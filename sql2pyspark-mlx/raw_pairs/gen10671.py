from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 29) & ((F.col("end_date") < "2024-08-22") | (F.col("status") == "PAUSED")))
    .groupBy(F.date_trunc("MONTH", "end_date").alias("end_month"))
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .orderBy("end_month")
    .limit(50)
)
