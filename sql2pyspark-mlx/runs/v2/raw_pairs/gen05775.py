from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-04-01", "2025-01-26") & (F.col("status") == "PAUSED"))
    .groupBy("plan", "status")
    .agg(F.count("*").alias("num_subscriptions"))
)
