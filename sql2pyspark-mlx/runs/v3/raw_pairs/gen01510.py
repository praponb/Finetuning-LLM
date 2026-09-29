from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.txn_type").isin(["DEBIT", "REFUND"]) & (F.year(F.col("a.opened_date")) == 2023))
    .select(
        F.col("t.amount"),
        F.col("t.is_flagged"),
        F.col("t.txn_time"),
        F.col("a.status"),
    )
)
