from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .select(
        F.col("a.account_type"),
        F.col("at.last_txn_time"),
        F.col("at.txn_type_list"),
    )
    .orderBy("last_txn_time")
)
