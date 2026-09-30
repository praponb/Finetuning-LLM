from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .select(
        F.col("transactions.txn_type"),
        F.col("accounts.status"),
        F.col("accounts.customer_id"),
        F.col("accounts.account_type"),
    )
)
