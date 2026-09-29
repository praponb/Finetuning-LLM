from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", F.month("opened_date").alias("opened_month"))
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
    .orderBy(F.col("opened_month").desc())
)
