from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["premium", "basic", "enterprise"]) & ~F.col("plan").isin(["enterprise", "premium", "standard"]))
    .groupBy(F.month("end_date").alias("end_month"))
    .agg(
        F.sum(F.when(F.col("status") == "PAUSED", 1).otherwise(0)).alias("paused_count"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
    .orderBy("end_month")
)
