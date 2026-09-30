from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(25000, 51000))
    .groupBy("account_type")
    .agg(
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.sum(F.when(F.col("status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance"),
    )
)
