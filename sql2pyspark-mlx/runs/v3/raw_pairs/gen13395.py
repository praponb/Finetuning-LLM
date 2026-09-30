from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .filter(F.col("transactions.account_id").isin([9, 35, 36, 38]))
    .select(
        F.col("accounts.account_type"),
        F.col("accounts.customer_id"),
        F.col("accounts.opened_date"),
        F.col("transactions.account_id"),
        F.col("transactions.merchant"),
        F.col("transactions.txn_type"),
    )
)
