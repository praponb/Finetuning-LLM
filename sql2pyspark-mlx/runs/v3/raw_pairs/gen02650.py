from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") < 53000)
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_count"),
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
        F.count("balance").alias("balance_count"),
    )
    .orderBy("status")
)
