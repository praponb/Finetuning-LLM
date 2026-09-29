from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.sum("balance").alias("total_balance"),
        F.min("balance").alias("min_balance"),
        F.avg("balance").alias("avg_balance"),
    )
    .orderBy("min_balance", "account_type")
    .limit(1)
)
