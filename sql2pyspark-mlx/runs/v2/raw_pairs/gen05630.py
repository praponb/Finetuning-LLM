from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.account_id").isin([1, 15]))
    .select(
        F.col("t.txn_time"),
        F.col("t.merchant"),
        F.col("t.txn_type"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("a.status"),
    )
)
