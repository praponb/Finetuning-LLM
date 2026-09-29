from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.count("balance").alias("balance_count"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"),
    )
)
