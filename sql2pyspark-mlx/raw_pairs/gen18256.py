from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.month(F.col("a.opened_date")).isin([5, 7, 9]))
    .groupBy(F.col("a.id"), F.col("a.account_type"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.max(F.col("t.txn_time")).alias("last_txn_time"),
        F.countDistinct(F.col("t.amount")).alias("unique_txn_types"),
    )
    .orderBy(F.col("transaction_count").desc())
)
