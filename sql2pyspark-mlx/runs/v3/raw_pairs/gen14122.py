from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") == "FEE") & (F.col("txn_time") >= "2025-07-09"))
    .groupBy("account_id", "txn_type")
    .agg(F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"))
    .filter(F.col("fee_count") < 42)
    .orderBy("fee_count", F.col("account_id").desc())
)
