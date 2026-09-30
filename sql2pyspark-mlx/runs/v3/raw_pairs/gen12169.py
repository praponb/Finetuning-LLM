from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
    .orderBy("plan", F.col("monthly_fee_count").desc())
)
