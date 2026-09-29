from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.balance") > 56000)
    .select(
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("t.txn_type"),
        F.col("t.txn_time"),
        F.col("t.amount"),
    )
)
