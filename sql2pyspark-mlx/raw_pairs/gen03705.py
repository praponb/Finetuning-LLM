from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.year("opened_date") == 2025)
    .groupBy("status", "account_type")
    .agg(
        F.sum(F.when(F.col("status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance"),
        F.sum("balance").alias("total_balance"),
        F.min("balance").alias("min_balance"),
    )
)
