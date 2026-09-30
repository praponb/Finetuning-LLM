from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(120, 190))
    .select("id", "monthly_fee", F.lower("plan").alias("plan_lower"))
)
