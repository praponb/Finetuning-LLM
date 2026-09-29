from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
    .filter(F.col("monthly_fee_count") >= 1)
)
