from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.quarter("end_date").alias("end_quarter"))
    .agg(F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"))
    .filter(F.col("active_count") >= 6)
    .orderBy(F.col("active_count").desc())
)
