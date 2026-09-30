from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.to_date("txn_time") >= "2025-10-01") | F.col("merchant").isin(["Amazon", "Starbucks"]))
    .groupBy("is_flagged", F.month("txn_time").alias("txn_month"))
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .filter(F.col("credit_count") > 2)
)
