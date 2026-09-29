from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.filter(F.col("merchant") == "Amazon").alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left_semi")
    .filter(F.col("accounts.balance").isNull())
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("accounts.account_type"),
        F.col("accounts.status"),
    )
)
