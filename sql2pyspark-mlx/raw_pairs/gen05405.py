from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(~F.col("t.is_flagged"))
    .groupBy(F.col("a.status"))
    .agg(
        F.min(F.col("t.txn_time")).alias("min_txn_time"),
        F.sum(F.when(F.col("t.merchant") == "Shell", 1).otherwise(0)).alias("shell_count"),
    )
)
