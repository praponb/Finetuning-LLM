from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") > 39000)
    .groupBy(
        F.when(F.col("balance") >= 72000, "high").when(F.col("balance") >= 30000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
        F.countDistinct("balance").alias("unique_account_types"),
    )
    .filter(F.col("open_count") < 6)
    .select("balance_bucket", "open_count", "unique_account_types")
)
