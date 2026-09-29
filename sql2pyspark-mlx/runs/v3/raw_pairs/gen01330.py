from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .select(
        F.col("t.id"),
        F.col("t.txn_time"),
        F.col("t.is_flagged"),
        F.col("a.account_type"),
    )
    .orderBy("account_type")
)
