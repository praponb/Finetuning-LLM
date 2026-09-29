from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .select(
        F.col("transactions.id"),
        F.col("transactions.account_id"),
        F.col("accounts.account_type"),
        F.col("accounts.opened_date"),
        F.col("accounts.customer_id"),
    )
)
