from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
    .orderBy("balance_range", "opened_month")
    .limit(10)
)
