from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.min("balance").alias("min_balance"),
        F.max("opened_date").alias("last_opened_date"),
    )
)
