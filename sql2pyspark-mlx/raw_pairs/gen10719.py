from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.balance").between(38000, 46000))
    .select(
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("t.merchant"),
        F.col("t.is_flagged"),
        F.col("t.txn_time"),
    )
    .orderBy(F.col("txn_time").desc(), F.col("status").desc())
)
