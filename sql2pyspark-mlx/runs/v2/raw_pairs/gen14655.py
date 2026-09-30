from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") <= 73000) | F.col("status").isNull())
    .groupBy(
        F.when(F.col("balance") >= 80000, "high").when(F.col("balance") >= 34000, "mid").otherwise("low").alias("balance_bucket"),
        "account_type",
    )
    .agg(F.avg("balance").alias("avg_balance"))
)
