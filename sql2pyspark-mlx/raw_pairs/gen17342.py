from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        "status",
        F.when(F.col("balance") >= 69000, "high").when(F.col("balance") >= 33000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
        F.sum(F.when(F.col("account_type") == "CHECKING", 1).otherwise(0)).alias("checking_count"),
    )
)
