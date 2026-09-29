from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.account_id").isin([10, 14, 26]))
    .groupBy(F.col("a.id"), F.col("a.status"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.min(F.col("t.txn_time")).alias("first_txn_time"),
        F.sum(F.col("t.amount")).alias("total_amount"),
    )
    .orderBy("transaction_count", F.col("id").desc())
)
