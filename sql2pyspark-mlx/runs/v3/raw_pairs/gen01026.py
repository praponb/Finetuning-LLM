from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "inner")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "left")
    .select(
        F.col("transactions.id"),
        F.col("transactions.merchant"),
        F.col("transactions.amount"),
        F.col("accounts.opened_date"),
        F.col("accounts.customer_id"),
        F.col("customers.name"),
        F.upper(F.col("accounts.status")).alias("status_upper"),
    )
)
