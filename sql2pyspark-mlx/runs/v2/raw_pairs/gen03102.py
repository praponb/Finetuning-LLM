from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Lazada", "7-Eleven", "Amazon"]) & (F.col("txn_time") >= "2023-08-31"))
    .select("id", "amount", "merchant", F.trim("txn_type").alias("clean_txn_type"))
    .orderBy(F.col("clean_txn_type").desc())
)
