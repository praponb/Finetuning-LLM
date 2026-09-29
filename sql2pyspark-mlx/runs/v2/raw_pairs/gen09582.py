from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .groupBy(F.col("a.status"))
    .agg(
        F.max(F.col("t.txn_time")).alias("last_txn_time"),
        (F.max(F.col("t.amount")) - F.min(F.col("t.amount"))).alias("amount_range"),
    )
    .orderBy(F.col("status").desc(), "last_txn_time")
)
