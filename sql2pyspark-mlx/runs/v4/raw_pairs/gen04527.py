from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 23000)
    .groupBy("account_type")
    .agg(
        F.sum(F.when(F.col("status") == "OPEN", F.col("balance")).otherwise(0)).alias("open_balance"),
    )
    .orderBy("account_type")
)
