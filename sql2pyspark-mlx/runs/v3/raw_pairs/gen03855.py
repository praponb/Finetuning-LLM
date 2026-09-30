from pyspark.sql import functions as F

valid_accounts = df_accounts.filter(F.month("opened_date").isin([3, 4, 9]))

status_summary = (
    valid_accounts
    .groupBy("status", "account_type")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.sum(F.when(F.col("status") == "OPEN", F.col("balance")).otherwise(0)).alias("open_balance"),
    )
)

result = (
    status_summary
    .filter(F.col("open_balance") >= 61)
    .orderBy("account_type")
)
