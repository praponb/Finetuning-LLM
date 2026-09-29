from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 6)
    .select("txn_type", "txn_month", "first_txn_time", "row_count")
    .orderBy("txn_month", F.col("row_count").desc(), "txn_type")
    .limit(20)
)
