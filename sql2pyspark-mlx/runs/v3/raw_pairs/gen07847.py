from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter((F.col("a.status") == "OPEN") | (F.col("t.txn_type") == "DEBIT"))
    .select(
        F.col("t.id"),
        F.col("t.merchant"),
        F.col("t.amount"),
        F.col("t.txn_time"),
        F.col("a.customer_id"),
        F.col("a.status"),
    )
)
