from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") >= "2025-04-17")
    .groupBy("status")
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
    .orderBy(F.col("status").desc())
    .limit(50)
)
