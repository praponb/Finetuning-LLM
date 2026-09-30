from pyspark.sql import functions as F

filtered_transactions = df_transactions.filter(F.col("amount") >= 3400)

transactions_stats = (
    filtered_transactions
    .groupBy("txn_type", F.to_date("txn_time").alias("txn_day"))
    .agg(
        F.max("amount").alias("max_amount"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    transactions_stats
    .select("txn_type", "max_amount", "min_amount")
    .orderBy("txn_type", F.col("min_amount").desc())
)
