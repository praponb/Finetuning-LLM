from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .filter((F.col("transactions.txn_type") == "FEE") & (F.to_date(F.col("transactions.txn_time")) < "2025-07-21"))
    .select(
        F.col("transactions.id"),
        F.col("transactions.account_id"),
        F.col("transactions.txn_time"),
        F.col("accounts.customer_id"),
    )
)
