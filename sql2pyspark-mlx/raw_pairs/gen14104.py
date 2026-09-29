from pyspark.sql import functions as F

valid_accounts = df_accounts.filter(F.col("customer_id").isNotNull())

accounts_stats = (
    valid_accounts
    .groupBy("status")
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
)

result = accounts_stats
