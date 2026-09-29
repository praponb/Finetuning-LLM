from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("plan").isin(["standard", "enterprise", "premium"]))
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .agg(
        F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0)).alias("active_count"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
    .filter(F.col("total_monthly_fee") >= 600)
    .select("start_month", "active_count", "total_monthly_fee")
    .orderBy(F.col("active_count").desc())
)
