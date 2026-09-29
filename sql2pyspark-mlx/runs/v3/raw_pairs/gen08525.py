from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]))
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.max("balance").alias("max_balance"))
    .filter(F.col("max_balance") >= 73000)
)
