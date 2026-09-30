from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_stats.alias("AccountStats"), F.col("accounts.id") == F.col("AccountStats.account_id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("AccountStats.txn_type_list"),
        F.col("AccountStats.total_amount"),
    )
    .orderBy("total_amount", "id")
    .limit(3)
)
