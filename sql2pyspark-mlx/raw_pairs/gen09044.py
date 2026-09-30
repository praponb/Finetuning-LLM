from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") < 6)
    .orderBy(F.col("txn_month").desc())
)
