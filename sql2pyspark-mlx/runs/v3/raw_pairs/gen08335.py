from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_totals.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("accounts.opened_date"),
        F.col("accounts.account_type"),
        F.col("t.txn_type_list"),
    )
)
