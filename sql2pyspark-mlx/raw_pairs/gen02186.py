from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select("id", "amount", F.trim("txn_type").alias("clean_txn_type"))
    .orderBy("id")
)
