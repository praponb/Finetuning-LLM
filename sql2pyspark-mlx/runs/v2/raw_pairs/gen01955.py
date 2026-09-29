from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "basic")
    .groupBy(F.date_trunc("MONTH", "start_date").alias("start_month"), "customer_id")
    .agg(
        F.min("start_date").alias("first_start_date"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
    .filter(F.col("avg_monthly_fee") < 90)
    .orderBy("avg_monthly_fee")
)
