from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.month("start_date").alias("start_month"))
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee")).otherwise(0)).alias("cancelled_monthly_fee"),
    )
    .orderBy(F.col("start_month").desc())
)
