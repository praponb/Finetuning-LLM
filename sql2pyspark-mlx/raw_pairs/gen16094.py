from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.avg("balance").alias("avg_balance"))
    .orderBy("avg_balance")
)
