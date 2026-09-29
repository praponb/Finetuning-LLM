from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .orderBy("min_opened_date")
)
