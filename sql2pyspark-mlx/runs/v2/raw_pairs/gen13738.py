from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.month("txn_time").alias("txn_month"))
    .agg(
        F.sum(F.when(F.col("merchant") == "Lazada", F.col("amount")).otherwise(0)).alias("lazada_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .filter(F.col("lazada_amount") >= 56)
    .select("txn_month", "lazada_amount", "stddev_amount")
)
