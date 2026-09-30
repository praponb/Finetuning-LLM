from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant", F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(F.count("*").alias("total_count"))
    .orderBy("txn_month")
)
