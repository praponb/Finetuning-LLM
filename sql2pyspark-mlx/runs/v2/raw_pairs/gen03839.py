from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(~F.col("t.is_flagged"))
    .select(
        F.col("a.opened_date"),
        F.col("t.amount"),
        F.col("t.txn_time"),
        F.col("t.is_flagged"),
    )
)
