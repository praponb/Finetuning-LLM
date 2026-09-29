from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant", "txn_type")
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") >= 1300)
    .orderBy("merchant")
)
