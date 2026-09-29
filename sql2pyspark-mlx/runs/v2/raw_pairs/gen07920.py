from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .select(
        F.col("t.id"),
        F.col("t.is_flagged"),
        F.col("t.merchant"),
        F.col("a.status"),
        F.col("a.account_type"),
    )
    .orderBy(F.col("status").desc())
)
