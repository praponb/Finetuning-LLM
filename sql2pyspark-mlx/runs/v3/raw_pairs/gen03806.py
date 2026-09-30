from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(2300, 3000) | (F.col("account_id") == 8))
    .groupBy(
        F.when(F.col("amount") >= 3700, "high").when(F.col("amount") >= 1800, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_count"),
        F.min("amount").alias("min_amount"),
    )
    .orderBy(F.col("starbucks_count").desc())
)
