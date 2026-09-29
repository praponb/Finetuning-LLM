from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("customers.segment").isin(["Corporate", "Consumer"]))
    .select(
        F.col("transactions.txn_type"),
        F.col("transactions.merchant"),
        F.col("accounts.status"),
        F.col("customers.name"),
    )
)
