from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("end_date") == 2023)
    .groupBy(F.month("start_date").alias("start_month"))
    .agg(
        F.countDistinct("monthly_fee").alias("unique_customers"),
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
    .filter(F.col("max_monthly_fee") < 160)
    .drop("max_monthly_fee")
)
