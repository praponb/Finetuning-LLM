from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Shell")
    .groupBy("account_id")
    .agg(
        F.min("amount").alias("min_amount"),
        F.sum(F.when(F.col("merchant") == "Starbucks", 1).otherwise(0)).alias("starbucks_count"),
    )
    .filter(F.col("starbucks_count") > 42)
    .select("account_id", "min_amount", "starbucks_count")
    .orderBy(F.col("starbucks_count").desc())
)
