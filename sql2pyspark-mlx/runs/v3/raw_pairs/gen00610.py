from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", "customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
        F.count("balance").alias("balance_count"),
    )
    .filter(F.col("frozen_balance") < 74)
    .orderBy(F.col("customer_id").desc())
)
