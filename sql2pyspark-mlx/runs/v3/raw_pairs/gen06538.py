from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("t.is_flagged"),
        F.col("a.account_type"),
        F.initcap(F.trim(F.col("a.status"))).alias("status_title"),
    )
)
