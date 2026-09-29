from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter((F.col("t.account_id") == 5) & F.col("a.customer_id").isin([10, 32, 33, 38]))
    .select(
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("t.txn_time"),
        F.col("t.amount"),
    )
)
