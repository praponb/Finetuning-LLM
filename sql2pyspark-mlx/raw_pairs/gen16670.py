from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.count("balance").alias("balance_count"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
    )
)
