from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .filter(F.col("accounts.opened_date").between("2023-08-01", "2025-07-24"))
    .select(
        F.col("transactions.id"),
        F.col("transactions.merchant"),
        F.col("transactions.account_id"),
        F.col("accounts.status"),
        F.col("accounts.account_type"),
        F.col("accounts.balance"),
    )
    .orderBy(F.col("account_id").desc(), F.col("id").desc())
)
