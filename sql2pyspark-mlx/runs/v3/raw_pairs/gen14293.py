from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.when(F.col("balance") >= 60000, "high").when(F.col("balance") >= 14000, "mid").otherwise("low").alias("balance_bucket"))
    .pivot("account_type", ["LOAN", "SAVINGS"])
    .agg(F.count("balance"))
    .orderBy("balance_bucket")
)
