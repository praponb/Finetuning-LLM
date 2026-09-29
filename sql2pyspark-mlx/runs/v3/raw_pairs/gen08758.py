from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") < 21000)
)
