from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("customer_id").isin([2, 5, 14, 39]) & (F.col("balance") < 40000)) | F.col("opened_date").between("2024-12-17", "2025-01-27"))
    .groupBy("account_type")
    .agg(
        F.max("balance").alias("max_balance"),
        F.sum(F.when(F.col("status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance"),
        F.avg("balance").alias("avg_balance"),
    )
)
