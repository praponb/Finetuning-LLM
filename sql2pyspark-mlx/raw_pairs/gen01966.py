from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(~F.col("t.is_flagged"))
    .select(
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("t.is_flagged"),
        F.col("t.amount"),
        F.col("t.txn_time"),
    )
)
