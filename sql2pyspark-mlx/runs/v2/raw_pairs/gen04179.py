from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(F.col("a.status"))
    .agg(
        (F.max(F.col("t.amount")) - F.min(F.col("t.amount"))).alias("amount_range"),
        F.sum(F.when(F.col("t.txn_type") == "REFUND", 1).otherwise(0)).alias("refund_count"),
    )
    .orderBy(F.col("amount_range").desc())
)
