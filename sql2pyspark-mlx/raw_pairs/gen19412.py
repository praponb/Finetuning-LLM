from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .select(
        F.col("accounts.status"),
        F.col("accounts.opened_date"),
        F.col("transactions.txn_type"),
        F.col("transactions.account_id"),
    )
    .orderBy(F.col("status").desc())
)
