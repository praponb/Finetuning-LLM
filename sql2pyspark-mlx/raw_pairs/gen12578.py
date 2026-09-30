from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter((F.col("amount") < 3500) & (F.col("amount") >= 3300) & F.col("account_id").isin([4, 8, 23, 31]))
    .withColumn("amount_scaled", F.round((F.col("amount") * 1.07), 2))
)

agg = (
    filtered_transactions
    .groupBy("is_flagged", "txn_type")
    .agg(F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_count"))
)

result = (
    agg
    .filter(F.col("starbucks_count") < 10)
    .orderBy(F.col("starbucks_count").desc())
)
