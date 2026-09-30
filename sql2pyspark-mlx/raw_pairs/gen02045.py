from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "right")
    .filter(F.col("transactions.txn_type") == "REFUND")
    .select(
        F.col("transactions.id"),
        F.col("transactions.is_flagged"),
        F.col("transactions.txn_time"),
        F.col("transactions.account_id"),
        F.col("accounts.balance"),
        F.col("accounts.opened_date"),
        F.col("accounts.status"),
    )
    .orderBy("id")
)
