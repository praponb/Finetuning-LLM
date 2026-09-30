from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.hour("txn_time").between(8, 11))
    .groupBy("account_id")
    .agg(
        F.max("txn_time").alias("latest_txn_time"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(transactions_by_account.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("accounts.balance"),
        F.col("t.latest_txn_time"),
        F.col("t.max_amount"),
    )
    .orderBy("id")
)
