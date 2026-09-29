from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .filter(F.col("transactions.is_flagged") & (F.col("transactions.amount") >= 3600))
    .select(
        F.col("accounts.account_type"),
        F.col("accounts.opened_date"),
        F.col("accounts.status"),
        F.col("transactions.txn_time"),
        F.col("transactions.txn_type"),
    )
)
