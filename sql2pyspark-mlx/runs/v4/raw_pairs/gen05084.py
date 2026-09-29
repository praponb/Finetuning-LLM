from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([11, 25, 29, 35]))
    .groupBy("status")
    .agg(F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"))
    .filter(F.col("failed_count") >= 9)
)
