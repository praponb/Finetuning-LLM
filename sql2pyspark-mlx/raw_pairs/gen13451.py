from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id", "is_flagged")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.min("txn_time").alias("min_txn_time"),
    )
    .filter(F.col("amount_range") >= 37)
    .select("account_id", "is_flagged", "amount_range", "min_txn_time")
    .orderBy("amount_range")
)
