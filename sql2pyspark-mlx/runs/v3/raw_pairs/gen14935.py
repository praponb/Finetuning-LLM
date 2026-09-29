from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 12) | (F.col("plan") == "enterprise"))
    .groupBy("status", "plan")
    .agg(F.max("monthly_fee").alias("max_monthly_fee"))
)
