from pyspark.sql import functions as F

result = (
    df_transactions
    .select("txn_type", F.trim("txn_type").alias("clean_txn_type"))
    .orderBy("clean_txn_type")
)
