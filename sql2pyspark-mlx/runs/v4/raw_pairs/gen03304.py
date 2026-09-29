from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("t.txn_time"),
        F.col("t.merchant"),
        F.col("t.is_flagged"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("a.account_type"),
    )
)
