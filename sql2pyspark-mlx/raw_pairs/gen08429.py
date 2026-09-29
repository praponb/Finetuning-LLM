from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("merchant") == "Lazada")
    .groupBy("account_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.min("txn_time").alias("first_txn_time"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("as.amount_range"),
        F.col("as.first_txn_time"),
    )
)
