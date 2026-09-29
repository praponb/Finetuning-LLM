from pyspark.sql import functions as F

src = (
    df_transactions
    .filter(~F.col("merchant").isin(["Lazada", "Starbucks", "Shell"]))
    .withColumn("amount_tier", F.when(F.col("amount") >= 4400, "High").when(F.col("amount") >= 600, "Medium").otherwise("Low"))
)

transactions_stats = (
    src
    .groupBy("merchant")
    .agg(F.min("txn_time").alias("min_txn_time"))
)

result = (
    transactions_stats
    .orderBy("min_txn_time", "merchant")
    .limit(5)
)
