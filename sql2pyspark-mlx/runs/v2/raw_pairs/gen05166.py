from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 89000, "high").when(F.col("balance") >= 24000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.max("balance").alias("max_balance"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") < 4)
)
