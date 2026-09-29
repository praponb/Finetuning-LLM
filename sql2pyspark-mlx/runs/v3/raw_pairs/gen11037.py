from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "PAUSED")
    .groupBy("status", F.quarter("end_date").alias("end_quarter"))
    .agg(F.count("*").alias("num_subscriptions"))
    .orderBy("num_subscriptions")
)
