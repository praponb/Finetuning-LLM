from pyspark.sql import functions as F

recent_accounts = df_accounts.filter((F.col("customer_id") == 19) & (F.col("balance") >= 29000))

accounts_stats = (
    recent_accounts
    .groupBy("account_type")
    .agg(
        F.max("balance").alias("max_balance"),
        F.sum("balance").alias("total_balance"),
    )
)

result = accounts_stats.select("account_type", "total_balance")
