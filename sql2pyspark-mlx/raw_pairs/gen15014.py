from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.month("opened_date").alias("opened_month"))
    .pivot("account_type", ["SAVINGS", "LOAN"])
    .agg(F.avg("balance"))
)
