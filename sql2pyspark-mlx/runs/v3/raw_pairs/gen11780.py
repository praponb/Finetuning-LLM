from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .filter(F.col("transactions.amount") >= 3900)
    .select(
        F.col("transactions.amount"),
        F.col("accounts.balance"),
        F.col("accounts.account_type"),
    )
)
