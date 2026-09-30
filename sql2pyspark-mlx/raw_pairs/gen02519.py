from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") <= 1900) | ~F.col("txn_type").isin(["CREDIT", "DEBIT"]))
    .groupBy("txn_type")
    .agg(F.max("txn_time").alias("last_txn_time"))
)
