from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 2) & F.col("customer_id").isin([19, 30]) & F.col("customer_id").isNull())
    .groupBy("status")
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
    .filter(F.col("monthly_fee_count") >= 2)
)
