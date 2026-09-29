from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(F.min("txn_time").alias("first_txn_time"))
    .orderBy(F.col("first_txn_time").desc())
)
