from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.max("balance").alias("max_balance"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
)
