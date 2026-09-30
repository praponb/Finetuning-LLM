from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.month(F.col("a.opened_date")).isin([3, 10, 11]))
    .select(
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("t.is_flagged"),
        F.col("t.amount"),
    )
)
