from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .select(
        F.col("transactions.amount"),
        F.col("transactions.txn_type"),
        F.col("accounts.opened_date"),
        F.col("accounts.account_type"),
        F.col("customers.name"),
        F.col("customers.segment"),
    )
    .orderBy(F.col("account_type").desc(), "name")
)
