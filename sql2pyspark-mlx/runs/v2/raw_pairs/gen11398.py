from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(F.col("txn_type").isin(["CREDIT", "DEBIT", "FEE"]) & (F.col("amount") >= 1200))
)

agg = (
    valid_transactions
    .groupBy(F.quarter("txn_time").alias("txn_quarter"))
    .agg(F.min("txn_time").alias("first_txn_time"))
)

result = agg
