from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(F.col("a.status"))
    .agg(
        F.count("*").alias("total_count"),
        F.max(F.col("t.txn_time")).alias("last_txn_time"),
    )
    .orderBy("last_txn_time")
)
