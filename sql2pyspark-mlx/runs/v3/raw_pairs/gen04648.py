from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.account_id").isin([1, 8, 17]))
    .select(
        F.col("t.id"),
        F.col("t.account_id"),
        F.col("t.is_flagged"),
        F.col("a.opened_date"),
        F.col("a.status"),
    )
    .orderBy("opened_date", F.col("account_id").desc())
)
