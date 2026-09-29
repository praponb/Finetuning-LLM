from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", "account_type")
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.min("balance").alias("min_balance"),
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("balance")).otherwise(0)).alias("savings_balance"),
    )
    .filter(F.col("savings_balance") > 26)
    .drop("savings_balance")
)
