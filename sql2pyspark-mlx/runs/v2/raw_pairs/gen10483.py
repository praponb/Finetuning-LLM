from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0))) / F.count("*")), 1).alias("pct_7_eleven"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(transactions_by_account.alias("transactions_by_account"), F.col("accounts.id") == F.col("transactions_by_account.account_id"), "left")
    .filter(F.col("transactions_by_account.pct_7_eleven") > 37)
    .select(
        F.col("accounts.balance"),
        F.col("accounts.account_type"),
        F.col("transactions_by_account.txn_type_values"),
        F.col("transactions_by_account.pct_7_eleven"),
    )
    .orderBy(F.col("balance").desc())
)
