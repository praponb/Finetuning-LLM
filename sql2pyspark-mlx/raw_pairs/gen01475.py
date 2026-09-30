from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", F.month("txn_time").alias("txn_month"))
    .agg(F.count("amount").alias("amount_count"))
    .filter(F.col("amount_count") > 9)
    .select("txn_type", "txn_month", "amount_count")
)
