from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .groupBy(
        F.when(F.col("a.balance") >= 87000, "high").when(F.col("a.balance") >= 30000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.avg(F.col("t.amount")).alias("avg_amount"))
    .orderBy("avg_amount", F.col("balance_bucket").desc())
)
