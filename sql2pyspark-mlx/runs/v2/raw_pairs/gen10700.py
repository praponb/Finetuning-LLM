from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 2200)
    .groupBy("is_flagged")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("amount")).otherwise(0)).alias("starbucks_amount"),
    )
    .filter(F.col("total_amount") >= 16000)
    .orderBy(F.col("total_amount").desc(), "is_flagged")
    .limit(50)
)
