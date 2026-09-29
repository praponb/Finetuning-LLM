from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", "status")
    .agg(
        F.sum(F.when(F.col("status") == "PAUSED", F.col("monthly_fee")).otherwise(0)).alias("paused_monthly_fee"),
        F.sum(F.when(F.col("plan") == "basic", F.col("monthly_fee")).otherwise(0)).alias("basic_monthly_fee"),
        F.count("*").alias("cnt"),
    )
    .orderBy("plan", "basic_monthly_fee")
)
