from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.txn_time") < "2025-01-01")
    .groupBy(
        F.when(F.col("a.balance") >= 59000, "high").when(F.col("a.balance") >= 31000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.min(F.col("t.amount")).alias("min_amount"),
        F.sum(F.when(F.col("t.merchant") == "Starbucks", 1).otherwise(0)).alias("starbucks_count"),
    )
)
