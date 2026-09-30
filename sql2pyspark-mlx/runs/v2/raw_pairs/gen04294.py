from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.month("opened_date").alias("opened_month"))
    .pivot("status", ["OPEN", "FROZEN", "CLOSED"])
    .agg(F.avg("balance"))
)
