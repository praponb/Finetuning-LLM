from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.max("balance").alias("max_balance"),
        F.avg("balance").alias("avg_balance"),
    )
)
