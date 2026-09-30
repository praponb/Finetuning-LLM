from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("id", "start_date", F.lower("status").alias("status_lower"))
    .orderBy(F.col("start_date").desc())
)
