from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "outer")
    .filter((F.col("accounts.status") == "OPEN") & F.col("transactions.txn_type").isin(["CREDIT", "DEBIT"]))
    .select(
        F.col("transactions.id"),
        F.col("transactions.amount"),
        F.col("transactions.account_id"),
        F.col("transactions.merchant"),
        F.col("accounts.account_type"),
        F.col("accounts.customer_id"),
    )
)
