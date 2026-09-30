from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") > "2025-10-01")
    .select("plan", "monthly_fee", "customer_id", F.lower("plan").alias("plan_lower"))
)
