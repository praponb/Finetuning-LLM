from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 87000, "high").when(F.col("balance") >= 33000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.max("balance").alias("max_balance"),
        F.min("balance").alias("min_balance"),
    )
    .orderBy("balance_bucket", "min_balance")
)
