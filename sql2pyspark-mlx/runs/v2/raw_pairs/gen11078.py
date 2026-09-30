from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & F.col("amount").between(2400, 3100))
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"), "txn_type")
    .agg(F.max("txn_time").alias("latest_txn_time"))
)
