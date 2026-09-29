from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.txn_time") < "2025-08-16")
    .groupBy(F.col("a.status"))
    .agg((F.max(F.col("t.amount")) - F.min(F.col("t.amount"))).alias("amount_range"))
)
