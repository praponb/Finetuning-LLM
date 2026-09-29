from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.is_flagged"))
    .groupBy(
        F.when(F.col("a.balance") >= 85000, "high").when(F.col("a.balance") >= 17000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.max(F.col("t.amount")).alias("max_amount"))
    .orderBy(F.col("max_amount").desc())
)
