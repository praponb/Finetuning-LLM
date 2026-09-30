from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") == "ACTIVE") | F.col("monthly_fee").between(130, 140))
    .select("customer_id", "status", F.trim("status").alias("clean_status"))
)
