from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") == "PAUSED") & (F.col("end_date") >= "2025-03-01"))
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
)
