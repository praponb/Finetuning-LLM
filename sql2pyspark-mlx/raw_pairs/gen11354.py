from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.when(F.col("balance") >= 83000, "high").when(F.col("balance") >= 23000, "mid").otherwise("low").alias("balance_bucket"))
    .pivot("account_type", ["CHECKING", "SAVINGS", "LOAN", "CREDIT"])
    .agg(F.avg("balance"))
)
