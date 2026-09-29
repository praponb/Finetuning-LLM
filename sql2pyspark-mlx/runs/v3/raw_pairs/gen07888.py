from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
        F.max("balance").alias("max_balance"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
)
