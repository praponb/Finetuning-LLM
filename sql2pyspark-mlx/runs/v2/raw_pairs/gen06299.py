from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("t.avg_amount"),
        F.col("t.txn_type_list"),
    )
)
