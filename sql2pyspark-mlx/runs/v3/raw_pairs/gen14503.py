from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(
        F.col("a.status"),
        F.col("a.balance"),
        F.col("tba.min_txn_time"),
        F.col("tba.txn_type_values"),
    )
)
