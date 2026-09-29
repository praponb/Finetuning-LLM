from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", "account_type")
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.sum("balance").alias("total_balance"),
        F.avg("balance").alias("avg_balance"),
    )
    .orderBy("total_balance", F.col("account_type").desc())
)
