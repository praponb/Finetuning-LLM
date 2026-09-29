from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .select("id", "txn_type", "is_flagged", F.trim("txn_type").alias("clean_txn_type"))
    .orderBy("txn_type")
)
