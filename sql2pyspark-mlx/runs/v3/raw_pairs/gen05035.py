from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
    .filter(F.col("balance_range") < 53)
    .select("opened_month", "balance_range")
    .orderBy("balance_range", "opened_month")
    .limit(5)
)
