from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.min("opened_date").alias("min_opened_date"),
        F.max("opened_date").alias("latest_opened_date"),
    )
    .orderBy("latest_opened_date")
)
