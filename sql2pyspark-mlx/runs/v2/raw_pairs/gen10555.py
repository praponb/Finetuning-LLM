from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter((F.to_date("txn_time") >= "2023-07-04") & F.hour("txn_time").between(10, 13))
)

transactions_stats = (
    filtered_transactions
    .groupBy(F.hour("txn_time").alias("txn_hour"))
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    transactions_stats
    .filter(F.col("avg_amount") >= 62)
    .select("txn_hour", "avg_amount")
    .orderBy("txn_hour")
)
