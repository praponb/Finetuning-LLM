from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([8, 14]) & (F.col("status") == "OPEN"))
    .groupBy("customer_id", "status")
    .agg(F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"))
)
