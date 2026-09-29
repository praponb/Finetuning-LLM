from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.balance").between(24000, 68000))
    .select(
        F.col("t.id"),
        F.col("t.merchant"),
        F.col("t.txn_time"),
        F.col("a.opened_date"),
    )
)
