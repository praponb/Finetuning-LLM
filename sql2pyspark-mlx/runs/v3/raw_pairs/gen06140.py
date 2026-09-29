from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(150, 240))
    .groupBy(F.month("start_date").alias("start_month"))
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee")).otherwise(0)).alias("cancelled_monthly_fee"),
        F.countDistinct("monthly_fee").alias("unique_plans"),
    )
    .orderBy("monthly_fee_range", F.col("cancelled_monthly_fee").desc(), "start_month")
    .limit(5)
)
