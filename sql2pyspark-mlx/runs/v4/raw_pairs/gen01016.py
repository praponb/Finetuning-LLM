from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([6, 11]) & (F.col("customer_id") == 39) & (F.col("end_date") >= "2024-02-01"))
    .groupBy(F.date_trunc("MONTH", "end_date").alias("end_month"))
    .agg(
        F.sum(F.when(F.col("status") == "ACTIVE", F.col("monthly_fee")).otherwise(0)).alias("active_monthly_fee"),
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
    .filter(F.col("total_monthly_fee") > 560)
    .drop("total_monthly_fee")
    .orderBy("max_monthly_fee", "end_month")
    .limit(10)
)
