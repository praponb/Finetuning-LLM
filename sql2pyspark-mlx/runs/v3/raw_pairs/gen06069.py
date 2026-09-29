from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .filter((F.col("t.txn_type") == "CREDIT") & F.col("a.customer_id").isin([20, 24, 27]))
    .select(
        F.col("t.id"),
        F.col("t.txn_time"),
        F.col("t.merchant"),
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.col("a.status"),
    )
)
