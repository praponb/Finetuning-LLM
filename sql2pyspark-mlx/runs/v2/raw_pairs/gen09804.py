from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([2, 4, 12]))
    .groupBy("customer_id", F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(
        F.sum(F.when(F.col("status") == "PAUSED", F.col("monthly_fee")).otherwise(0)).alias("paused_monthly_fee"),
    )
    .orderBy(F.col("customer_id").desc())
)
