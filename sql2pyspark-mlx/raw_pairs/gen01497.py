from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .select(
        F.col("transactions.account_id"),
        F.col("transactions.merchant"),
        F.col("transactions.is_flagged"),
        F.col("accounts.status"),
        F.col("accounts.opened_date"),
        F.col("accounts.account_type"),
    )
)
