from pyspark.sql import functions as F

result = (
    df_subscriptions
    .withColumn("days_since_start_date", F.datediff(F.current_date(), "start_date"))
    .withColumn("status_prefix", F.substring("status", 1, 4))
    .orderBy(F.col("customer_id").desc())
)
