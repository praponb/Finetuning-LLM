from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.customer_id").isin([3, 22]) | (F.col("t.amount") < 2700))
    .select(
        F.col("t.txn_time"),
        F.col("t.txn_type"),
        F.col("t.amount"),
        F.col("a.status"),
    )
)
