from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") != "ACTIVE")
    .groupBy("plan")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee")).otherwise(0)).alias("cancelled_monthly_fee"),
    )
    .filter(F.col("cancelled_monthly_fee") >= 44)
)
