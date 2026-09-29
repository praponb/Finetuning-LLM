from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .select(
        F.col("t.txn_time"),
        F.col("t.amount"),
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("a.customer_id"),
    )
)
