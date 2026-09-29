from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([2, 8, 12]))
    .groupBy(F.date_trunc("MONTH", "start_date").alias("start_month"))
    .agg(
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee")).otherwise(0)).alias("premium_monthly_fee"),
        F.min("end_date").alias("first_end_date"),
        F.max("end_date").alias("last_end_date"),
    )
    .filter(F.col("premium_monthly_fee") > 75)
)
