from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") < 73000)
    .groupBy("account_type")
    .agg(
        F.sum("balance").alias("total_balance"),
        F.avg("balance").alias("avg_balance"),
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("balance")).otherwise(0)).alias("savings_balance"),
        F.countDistinct("balance").alias("unique_statuss"),
    )
    .filter(F.col("unique_statuss") > 3)
    .drop("unique_statuss")
    .orderBy(F.col("savings_balance").desc(), "avg_balance")
)
