from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Lazada")
    .withColumn("days_since_txn_time", F.datediff(F.current_date(), "txn_time"))
    .orderBy(F.col("days_since_txn_time").desc(), "txn_time")
)
