from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
        F.sum("balance").alias("total_balance"),
    )
    .filter(F.col("frozen_balance") >= 66)
    .orderBy("frozen_balance", "account_type")
    .limit(10)
)
