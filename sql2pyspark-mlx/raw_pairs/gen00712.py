from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .filter((F.col("accounts.opened_date") <= "2024-07-23") & (F.col("transactions.merchant") != "Amazon"))
    .select(
        F.col("transactions.id"),
        F.col("transactions.txn_time"),
        F.col("accounts.customer_id"),
        F.col("accounts.account_type"),
    )
)
