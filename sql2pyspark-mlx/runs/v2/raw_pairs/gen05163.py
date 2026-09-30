from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("end_date") >= 2024)
    .groupBy("status", "customer_id")
    .agg(
        F.countDistinct("monthly_fee").alias("unique_plans"),
        F.min("end_date").alias("first_end_date"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
    .filter(F.col("max_monthly_fee") >= 110)
    .drop("max_monthly_fee")
    .orderBy("status", F.col("unique_plans").desc())
)
