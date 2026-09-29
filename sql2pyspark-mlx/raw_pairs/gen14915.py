from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(F.col("account_id").isin([1, 3, 29]))
    .withColumn("merchant_txn_type_key", F.concat_ws("-", "merchant", "txn_type"))
)

transactions_stats = (
    valid_transactions
    .groupBy("txn_type")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    transactions_stats
    .filter(F.col("max_amount") < 3700)
    .orderBy("max_amount")
)
