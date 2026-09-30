from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") <= 39000)
    .groupBy(F.month("opened_date").alias("opened_month"))
    .pivot("status", ["FROZEN", "CLOSED", "OPEN"])
    .agg(F.avg("balance"))
    .orderBy("opened_month")
)
