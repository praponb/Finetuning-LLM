from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter(((F.col("balance") < 31000) & (F.col("balance") >= 61000)) | (F.col("balance") <= 19000))
)

agg = (
    filtered_accounts
    .groupBy(
        F.when(F.col("balance") >= 68000, "high").when(F.col("balance") >= 21000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.count("*").alias("account_count"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
)

result = (
    agg
    .filter(F.col("stddev_balance") >= 56)
    .select("balance_bucket", "account_count", "stddev_balance")
    .orderBy(F.col("stddev_balance").desc())
)
