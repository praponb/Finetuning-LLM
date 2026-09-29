from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy(F.col("max_amount").desc())
)
