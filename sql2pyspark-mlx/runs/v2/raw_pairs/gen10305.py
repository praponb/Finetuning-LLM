from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "outer")
    .select(
        F.col("t.id"),
        F.col("t.account_id"),
        F.col("t.merchant"),
        F.col("t.txn_time"),
        F.col("a.status"),
        F.col("a.opened_date"),
    )
)
