from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter(F.col("amount") <= 3700)
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("N/A")))
)

transactions_stats = (
    recent_transactions
    .groupBy("merchant")
    .agg(
        F.min("amount").alias("min_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    transactions_stats
    .filter(F.col("min_amount") > 2900)
    .select("merchant", "min_amount", "avg_amount")
    .orderBy("merchant")
)
