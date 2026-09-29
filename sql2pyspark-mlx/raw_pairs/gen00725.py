from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter(((F.col("balance") >= 46000) & F.col("balance").between(42000, 45000)) | F.col("status").isin(["OPEN", "FROZEN"]))
)

accounts_stats = (
    valid_accounts
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0)).alias("frozen_count"),
        F.avg("balance").alias("avg_balance"),
    )
)

result = accounts_stats.filter(F.col("frozen_count") >= 61)
