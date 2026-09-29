from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
    )
    .filter(F.col("frozen_balance") >= 57)
    .orderBy(F.col("account_type").desc())
)
