from pyspark.sql import functions as F

valid_accounts = df_accounts.filter(F.col("opened_date") < "2024-03-01")

agg = (
    valid_accounts
    .groupBy(
        F.when(F.col("balance") >= 70000, "high").when(F.col("balance") >= 22000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance"),
    )
)

result = (
    agg
    .filter(F.col("closed_balance") < 57)
    .select("balance_bucket", "closed_balance")
)
