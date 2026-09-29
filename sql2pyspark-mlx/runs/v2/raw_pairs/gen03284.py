from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 69000, "high").when(F.col("balance") >= 7000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.sum("balance").alias("total_balance"),
    )
    .orderBy("balance_range")
)
