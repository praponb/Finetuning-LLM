from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(
        F.col("a.status"),
        F.col("a.balance"),
        F.col("t.merchant"),
        F.col("t.account_id"),
        (F.datediff(F.current_date(), F.col("t.txn_time")) / 7).alias("weeks_since_txn_time"),
    )
    .orderBy(F.col("status").desc())
)
