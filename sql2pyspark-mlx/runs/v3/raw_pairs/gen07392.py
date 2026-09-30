from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("plan").isin(["premium", "standard"]) & F.col("status").isin(["PAUSED", "CANCELLED", "EXPIRED"]) & (F.col("monthly_fee") < 80))
    .groupBy("plan", "status")
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
        F.countDistinct("monthly_fee").alias("unique_customers"),
    )
    .filter(F.col("cancelled_count") < 55)
)
