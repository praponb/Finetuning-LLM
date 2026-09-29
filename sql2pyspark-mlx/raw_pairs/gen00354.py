from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("accounts.opened_date") <= "2024-03-01")
    .select(
        F.col("transactions.id"),
        F.col("transactions.amount"),
        F.col("accounts.opened_date"),
        F.col("customers.email"),
    )
    .orderBy("opened_date")
)
