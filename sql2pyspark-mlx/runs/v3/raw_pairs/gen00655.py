from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.status").isNotNull())
    .groupBy(F.col("a.id"), F.col("a.account_type"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.sort_array(F.collect_set(F.col("t.txn_type"))).alias("txn_type_list"),
        F.min(F.col("t.txn_time")).alias("min_txn_time"),
    )
    .orderBy(F.col("transaction_count").desc())
)
