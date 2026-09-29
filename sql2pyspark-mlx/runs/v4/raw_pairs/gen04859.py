from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").isNotNull() & (F.col("account_type") == "CHECKING"))
    .groupBy(
        F.when(F.col("balance") >= 69000, "high").when(F.col("balance") >= 10000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.avg("balance").alias("avg_balance"))
    .orderBy("avg_balance")
)
