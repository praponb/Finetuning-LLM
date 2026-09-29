from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.hour("txn_time").alias("txn_hour"))
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") > 13000)
)
