from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(100, 160) | (F.col("status") == "EXPIRED"))
    .groupBy("status", "plan")
    .agg(F.max("monthly_fee").alias("max_monthly_fee"))
)
