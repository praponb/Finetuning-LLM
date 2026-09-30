from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.is_flagged"))
    .groupBy(F.col("a.status"))
    .agg(
        F.count(F.when(F.col("t.txn_type") == "REFUND", 1)).alias("refund_count"),
        (F.max(F.col("t.amount")) - F.min(F.col("t.amount"))).alias("amount_range"),
    )
    .filter(F.col("refund_count") > 4)
    .orderBy("status", "refund_count")
)
