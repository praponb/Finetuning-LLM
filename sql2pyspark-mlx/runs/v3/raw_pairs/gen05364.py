from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.sum("balance").alias("total_balance"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.countDistinct("balance").alias("unique_customers"),
    )
    .filter(F.col("avg_balance") >= 73)
)
