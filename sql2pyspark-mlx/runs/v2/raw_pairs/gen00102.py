from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"),
        F.max("txn_time").alias("last_txn_time"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_totals.alias("account_totals"), F.col("accounts.id") == F.col("account_totals.account_id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.account_type"),
        F.col("account_totals.fee_count"),
        F.col("account_totals.last_txn_time"),
    )
    .orderBy("last_txn_time", "account_type")
)
