from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([4, 6, 16, 29]) & ((F.col("balance") < 27000) | F.col("customer_id").isNull()))
    .groupBy("status", "account_type")
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
        F.avg("balance").alias("avg_balance"),
    )
)
