from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(40000, 51000) | (F.col("status") == "CLOSED"))
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance"),
    )
    .filter(F.col("closed_balance") >= 68)
)
