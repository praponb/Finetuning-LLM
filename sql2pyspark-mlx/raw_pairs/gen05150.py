from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") >= "2023-10-26")
    .groupBy(F.month("end_date").alias("end_month"))
    .agg(
        F.max("end_date").alias("last_end_date"),
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee")).otherwise(0)).alias("premium_monthly_fee"),
    )
    .orderBy("last_end_date", "end_month")
    .limit(5)
)
