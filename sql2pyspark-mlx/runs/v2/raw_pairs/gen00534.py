from pyspark.sql import functions as F

base = df_transactions.filter(~(F.col("is_flagged") & F.col("is_flagged")))

transactions_stats = (
    base
    .groupBy("merchant")
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    transactions_stats
    .filter(F.col("avg_amount") > 60)
    .select("merchant", "avg_amount")
)
