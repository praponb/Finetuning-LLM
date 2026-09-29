from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .select(F.col("t.is_flagged"), F.col("a.status"))
    .orderBy(F.col("status").desc())
)
