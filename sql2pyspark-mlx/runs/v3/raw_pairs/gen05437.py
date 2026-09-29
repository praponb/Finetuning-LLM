from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type", "status")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0)).alias("open_count"),
    )
    .orderBy("status", "account_type")
)
