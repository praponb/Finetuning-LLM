from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type", "status")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.min("balance").alias("min_balance"),
    )
    .orderBy("account_type")
)
