from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "inner")
    .filter((F.col("accounts.account_type") == "SAVINGS") & F.hour(F.col("transactions.txn_time")).between(7, 10))
    .select(
        F.col("transactions.id"),
        F.col("transactions.amount"),
        F.col("accounts.opened_date"),
        F.col("customers.city"),
        F.col("customers.is_active"),
    )
    .orderBy("opened_date")
)
