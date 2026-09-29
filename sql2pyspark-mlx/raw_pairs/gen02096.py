from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(F.count("*").alias("transaction_count"))
    .orderBy(F.col("transaction_count").desc(), "txn_type")
    .limit(20)
)
