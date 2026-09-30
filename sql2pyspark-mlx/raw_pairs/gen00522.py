from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cnt").desc())
)
