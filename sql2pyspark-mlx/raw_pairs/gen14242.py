from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(F.min("amount").alias("min_amount"))
    .orderBy(F.col("min_amount").desc(), "txn_type")
)
