from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") != "FEE") & (F.col("txn_time") >= "2025-08-01") & (F.col("amount") <= 2600))
    .select("account_id", "amount", F.trim("txn_type").alias("clean_txn_type"))
    .orderBy("clean_txn_type")
)
