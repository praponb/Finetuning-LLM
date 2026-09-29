from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"))
)
