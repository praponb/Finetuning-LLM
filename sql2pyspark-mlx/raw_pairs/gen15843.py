from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.min("balance").alias("min_balance"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_rows"),
    )
    .orderBy("min_balance")
)
