from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .orderBy("last_txn_time", "amount_range")
)
