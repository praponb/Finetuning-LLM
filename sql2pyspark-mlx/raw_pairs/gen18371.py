from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .filter((F.col("accounts.account_type") != "CREDIT") & (F.col("accounts.balance") <= 51000))
    .select(
        F.col("accounts.account_type"),
        F.col("accounts.opened_date"),
        F.col("transactions.account_id"),
        F.col("transactions.is_flagged"),
    )
    .orderBy(F.col("opened_date").desc())
)
