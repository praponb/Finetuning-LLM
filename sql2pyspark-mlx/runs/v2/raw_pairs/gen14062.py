from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.month("txn_time").alias("txn_month"))
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") < 21000)
)
