from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([24, 27, 29]) | F.col("customer_id").isNotNull())
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.avg("balance").alias("avg_balance"))
)
