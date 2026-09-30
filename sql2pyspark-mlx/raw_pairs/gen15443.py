from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 190) & (F.col("monthly_fee") >= 110))
    .groupBy("plan")
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee")).otherwise(0)).alias("cancelled_monthly_fee"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
)
