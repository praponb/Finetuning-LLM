from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") > "2024-12-01")
    .groupBy("customer_id", F.month("end_date").alias("end_month"))
    .agg(F.countDistinct("monthly_fee").alias("unique_plans"))
    .filter(F.col("unique_plans") >= 8)
    .select("customer_id", "end_month", "unique_plans")
    .orderBy(F.col("end_month").desc())
)
