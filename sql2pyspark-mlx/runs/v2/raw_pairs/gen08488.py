from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left_semi")
    .filter(F.col("accounts.status").isNull())
    .select(
        F.col("accounts.id"),
        F.col("accounts.status"),
        F.col("accounts.balance"),
        F.col("accounts.account_type"),
    )
)
