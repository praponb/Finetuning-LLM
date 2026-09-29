from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter(((F.col("txn_time") < "2024-08-01") & F.col("txn_type").isin(["REFUND", "DEBIT", "CREDIT"])) | F.col("merchant").isin(["Amazon", "Shell"]))
    .withColumn("merchant_length", F.length("merchant"))
)

transactions_stats = (
    filtered_transactions
    .groupBy("is_flagged")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    transactions_stats
    .filter(F.col("avg_amount") >= 33)
    .orderBy(F.col("avg_amount").desc())
)
