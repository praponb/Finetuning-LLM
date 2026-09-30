from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("a.opened_date"),
        F.col("t.txn_type"),
        F.col("t.txn_time"),
        F.col("t.is_flagged"),
    )
    .orderBy("opened_date", F.col("customer_id").desc())
)
