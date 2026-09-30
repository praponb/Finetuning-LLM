from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", "account_type")
    .agg(F.avg("balance").alias("avg_balance"))
)
