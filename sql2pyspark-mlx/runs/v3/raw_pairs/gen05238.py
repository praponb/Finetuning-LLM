from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .filter(F.col("transactions.amount").between(2700, 3100))
    .select(
        F.col("accounts.customer_id"),
        F.col("transactions.is_flagged"),
        F.col("transactions.amount"),
        F.col("transactions.txn_type"),
    )
)
