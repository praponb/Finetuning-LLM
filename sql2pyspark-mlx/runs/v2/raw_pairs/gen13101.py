from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", "customer_id", F.quarter("end_date").alias("end_quarter"))
    .agg(F.sum(F.when(F.col("plan") == "basic", 1).otherwise(0)).alias("basic_count"))
    .orderBy("status")
)
