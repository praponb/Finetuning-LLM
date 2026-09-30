from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 58000, "high").when(F.col("balance") >= 37000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_open"),
    )
    .filter(F.col("pct_open") >= 65)
)
