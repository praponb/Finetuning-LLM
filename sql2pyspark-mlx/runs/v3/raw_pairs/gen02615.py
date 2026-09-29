from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["FEE", "DEBIT"]) & (F.col("amount") < 1200))
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .orderBy("min_txn_time", "amount_range")
)
