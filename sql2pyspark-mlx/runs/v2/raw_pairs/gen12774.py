from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 35000)
    .groupBy("account_type", "status")
    .agg(
        F.sum(F.when(F.col("account_type") == "LOAN", 1).otherwise(0)).alias("loan_count"),
        F.min("opened_date").alias("min_opened_date"),
        F.sum(F.when(F.col("account_type") == "LOAN", F.col("balance")).otherwise(0)).alias("loan_balance"),
    )
    .filter(F.col("loan_balance") > 35)
)
