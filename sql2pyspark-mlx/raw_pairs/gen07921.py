from pyspark.sql import functions as F

valid_transactions = df_transactions.filter(F.col("amount") <= 2300)

summary = (
    valid_transactions
    .groupBy("is_flagged", "merchant")
    .agg(
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("amount")).otherwise(0)).alias("amazon_amount"),
        F.count("*").alias("row_count"),
    )
)

result = (
    summary
    .filter(F.col("row_count") >= 9)
    .orderBy("amazon_amount", F.col("merchant").desc())
)
