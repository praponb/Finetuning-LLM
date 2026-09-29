from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", F.month("txn_time").alias("txn_month"))
    .agg(F.sum(F.when(F.col("merchant") == "Grab", 1).otherwise(0)).alias("grab_count"))
)
