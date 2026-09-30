from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", F.month("paid_at").alias("paid_month"))
    .agg(F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"))
    .filter(F.col("failed_count") < 4)
    .orderBy("method", F.col("failed_count").desc())
)
