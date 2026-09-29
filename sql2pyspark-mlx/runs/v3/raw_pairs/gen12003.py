from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .select(
        F.col("accounts.balance"),
        F.col("accounts.account_type"),
        F.col("transactions.is_flagged"),
        F.col("transactions.merchant"),
        F.col("transactions.amount"),
    )
)
