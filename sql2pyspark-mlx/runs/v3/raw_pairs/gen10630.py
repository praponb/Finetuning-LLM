from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("merchant")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("amount")).otherwise(0)).alias("starbucks_amount"),
    )
    .filter(F.col("starbucks_amount") < 58)
    .orderBy("merchant")
)
