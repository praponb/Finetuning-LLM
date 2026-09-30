from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["PAUSED", "EXPIRED"]) & (F.col("start_date") > "2024-03-01"))
    .groupBy("status")
    .agg(
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("status") == "ACTIVE", F.col("monthly_fee")).otherwise(0)).alias("active_monthly_fee"),
    )
    .orderBy(F.col("total_count").desc())
)
