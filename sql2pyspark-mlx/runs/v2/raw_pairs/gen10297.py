from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 78000, "high").when(F.col("balance") >= 20000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.count("balance").alias("balance_count"),
    )
    .filter(F.col("balance_count") >= 2)
    .orderBy("balance_count", "balance_bucket")
    .limit(10)
)
