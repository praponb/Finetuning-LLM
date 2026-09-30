from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(F.min("txn_time").alias("first_txn_time"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_totals.alias("account_totals"), F.col("accounts.id") == F.col("account_totals.account_id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.balance"),
        F.col("account_totals.first_txn_time"),
    )
    .orderBy(F.col("id").desc())
)
