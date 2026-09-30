from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
        F.min("balance").alias("min_balance"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("frozen_balance") >= 69)
)
