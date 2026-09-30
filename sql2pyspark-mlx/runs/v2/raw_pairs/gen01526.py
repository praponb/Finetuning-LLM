from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.status") == "CLOSED")
    .groupBy(F.col("a.account_type"))
    .agg(
        F.count("*").alias("transaction_count"),
        F.min(F.col("t.txn_time")).alias("first_txn_time"),
    )
    .orderBy(F.col("transaction_count").desc())
)
