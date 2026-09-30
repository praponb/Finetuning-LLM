from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.txn_time") < "2024-11-15")
    .groupBy(
        F.when(F.col("a.balance") >= 58000, "high").when(F.col("a.balance") >= 15000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.sum(F.col("t.amount")).alias("total_amount"),
        F.max(F.col("t.amount")).alias("max_amount"),
    )
    .orderBy("balance_bucket")
)
