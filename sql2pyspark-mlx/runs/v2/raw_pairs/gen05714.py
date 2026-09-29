from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("accounts.opened_date") <= "2024-04-26")
    .select(
        F.col("transactions.account_id"),
        F.col("transactions.is_flagged"),
        F.col("accounts.opened_date"),
        F.col("accounts.account_type"),
        F.col("customers.is_active"),
    )
)
