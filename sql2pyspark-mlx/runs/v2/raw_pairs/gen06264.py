from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .groupBy(F.col("a.account_type"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.avg(F.col("t.amount")).alias("avg_amount"),
        F.min(F.col("t.txn_time")).alias("first_txn_time"),
    )
    .orderBy(F.col("avg_amount").desc())
)
