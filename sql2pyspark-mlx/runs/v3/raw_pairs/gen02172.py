from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 65000, "high").when(F.col("balance") >= 26000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.max("balance").alias("max_balance"),
        F.sum("balance").alias("total_balance"),
    )
    .filter(F.col("total_balance") < 423000)
    .select("balance_bucket", "max_balance", "total_balance")
)
