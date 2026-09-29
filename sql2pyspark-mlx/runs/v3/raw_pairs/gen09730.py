from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.year(F.col("a.opened_date")) >= 2025)
    .groupBy(F.col("a.id"), F.col("a.status"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.max(F.col("t.amount")).alias("max_amount"),
        F.min(F.col("t.txn_time")).alias("min_txn_time"),
    )
    .filter(F.col("max_amount") >= 2900)
    .orderBy("status")
)
