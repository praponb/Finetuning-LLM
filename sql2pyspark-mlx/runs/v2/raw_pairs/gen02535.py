from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(F.col("a.id"), F.col("a.status"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.sort_array(F.collect_set(F.col("t.txn_type"))).alias("txn_type_list"),
    )
)
