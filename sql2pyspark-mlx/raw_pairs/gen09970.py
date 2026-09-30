from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.countDistinct("amount").alias("unique_accounts"),
        F.min("txn_time").alias("first_txn_time"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_totals.alias("account_totals"), F.col("accounts.id") == F.col("account_totals.account_id"), "left")
    .select(
        F.col("accounts.status"),
        F.col("account_totals.unique_accounts"),
        F.col("account_totals.first_txn_time"),
    )
    .orderBy("unique_accounts")
)
