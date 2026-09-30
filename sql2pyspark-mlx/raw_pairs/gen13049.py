from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(
        F.col("a.status"),
        F.col("t.txn_type"),
        F.col("t.account_id"),
        F.col("t.merchant"),
    )
)
