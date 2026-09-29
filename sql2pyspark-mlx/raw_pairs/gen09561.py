from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 78000, "high").when(F.col("balance") >= 11000, "mid").otherwise("low").alias("balance_bucket"),
        "status",
        "account_type",
    )
    .agg(
        F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_count"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
)
