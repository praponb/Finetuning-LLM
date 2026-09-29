from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(
        F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
        F.min("txn_time").alias("min_txn_time"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") < 2400)
    .drop("max_amount")
)
