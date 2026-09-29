from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", F.month("start_date").alias("start_month"))
    .agg(
        F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0)).alias("expired_count"),
        F.sum(F.when(F.col("plan") == "basic", F.col("monthly_fee")).otherwise(0)).alias("basic_monthly_fee"),
        F.max("end_date").alias("last_end_date"),
    )
    .orderBy("basic_monthly_fee")
)
