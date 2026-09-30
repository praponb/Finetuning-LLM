from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("account_id") == 31) & (F.col("txn_time") >= "2024-05-01"))
    .groupBy("account_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_count"),
    )
    .filter(F.col("starbucks_count") > 4)
    .orderBy("avg_amount")
)
