from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .filter(F.col("accounts.opened_date") > "2024-04-29")
    .select(
        F.col("accounts.customer_id"),
        F.col("accounts.balance"),
        F.col("transactions.account_id"),
    )
)
