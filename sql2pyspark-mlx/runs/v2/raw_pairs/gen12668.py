from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("t.amount"),
        F.col("t.is_flagged"),
    )
)
