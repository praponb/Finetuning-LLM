from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "inner")
    .filter(F.col("accounts.opened_date").between("2025-03-25", "2025-07-01"))
    .select(
        F.col("accounts.status"),
        F.col("accounts.customer_id"),
        F.col("accounts.account_type"),
        F.col("transactions.txn_type"),
        F.col("transactions.is_flagged"),
    )
)
