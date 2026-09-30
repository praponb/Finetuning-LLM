from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("id", "monthly_fee", F.trim("status").alias("clean_status"))
    .orderBy(F.col("clean_status").desc())
)
