from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .select(
        F.col("accounts.opened_date"),
        F.col("accounts.account_type"),
        F.col("accounts.balance"),
        F.col("transactions.merchant"),
    )
)
