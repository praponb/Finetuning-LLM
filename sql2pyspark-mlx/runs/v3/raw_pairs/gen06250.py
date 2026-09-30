from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .filter(F.col("accounts.opened_date") >= "2024-09-01")
    .select(
        F.col("accounts.account_type"),
        F.col("accounts.status"),
        F.col("transactions.txn_type"),
        F.col("transactions.merchant"),
        F.col("transactions.is_flagged"),
    )
)
