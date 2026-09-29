from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 70) | F.col("customer_id").isin([11, 15, 29, 32]))
    .select("monthly_fee", "customer_id", F.lower("plan").alias("plan_lower"))
    .orderBy(F.col("monthly_fee").desc())
)
