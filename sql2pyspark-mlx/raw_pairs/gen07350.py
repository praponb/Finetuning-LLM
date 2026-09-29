from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.account_id").isin([5, 8, 34]))
    .select(
        F.col("t.id"),
        F.col("t.is_flagged"),
        F.col("t.account_id"),
        F.col("t.txn_time"),
        F.col("a.balance"),
    )
)
