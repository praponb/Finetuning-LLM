from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]) & ((F.col("balance") <= 20000) | (F.col("balance") < 42000)))
    .groupBy(
        "status",
        F.when(F.col("balance") >= 79000, "high").when(F.col("balance") >= 11000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"))
    .filter(F.col("closed_count") >= 5)
    .orderBy("status", "balance_bucket")
    .limit(50)
)
