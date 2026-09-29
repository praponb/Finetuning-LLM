from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(~F.col("status").isin(["FROZEN", "OPEN", "CLOSED"]) & (F.col("balance") <= 69000))
)

balance_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("balance") >= 81000, "high").when(F.col("balance") >= 14000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_count"),
    )
)

result = balance_bucket_summary.select("balance_bucket", "closed_count")
