from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"))
    .orderBy(F.col("frozen_count").desc(), "opened_month")
    .limit(3)
)
