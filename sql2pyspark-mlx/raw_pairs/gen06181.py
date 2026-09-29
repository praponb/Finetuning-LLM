from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter((F.col("merchant") == "Amazon") & F.col("merchant").isin(["Starbucks", "7-Eleven", "Grab"]))
)

transactions_stats = (
    valid_transactions
    .groupBy("is_flagged")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = transactions_stats
