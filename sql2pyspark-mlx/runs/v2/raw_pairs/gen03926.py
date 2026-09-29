from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.sum("balance").alias("total_balance"),
        F.sum(F.when(F.col("account_type") == "CHECKING", 1).otherwise(0)).alias("checking_count"),
        F.avg("balance").alias("avg_balance"),
    )
    .filter(F.col("avg_balance") < 39000)
    .drop("avg_balance")
)
