from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .groupBy(
        F.when(F.col("a.balance") >= 66000, "high").when(F.col("a.balance") >= 31000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg((F.max(F.col("t.amount")) - F.min(F.col("t.amount"))).alias("amount_range"))
    .filter(F.col("amount_range") >= 42)
    .orderBy("amount_range")
)
