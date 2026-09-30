from pyspark.sql import functions as F

filtered_transactions = df_transactions.filter(F.col("is_flagged"))

summary = (
    filtered_transactions
    .groupBy("is_flagged", F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_count"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0)).alias("debit_count"),
    )
)

result = summary.select("is_flagged", "txn_quarter", "debit_count")
