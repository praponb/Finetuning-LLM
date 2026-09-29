from pyspark.sql import functions as F

valid_accounts = df_accounts.filter(F.col("opened_date") <= "2025-09-01")

accounts_stats = (
    valid_accounts
    .groupBy("status")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
)

result = (
    accounts_stats
    .filter(F.col("avg_balance") >= 72000)
    .select("status", "stddev_balance")
)
