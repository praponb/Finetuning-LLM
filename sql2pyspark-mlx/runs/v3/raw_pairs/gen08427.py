from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(80, 200))
    .select("customer_id", F.trim("plan").alias("clean_plan"))
)
