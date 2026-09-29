from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg(
        F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0)).alias("7_eleven_count"),
        F.count("amount").alias("amount_count"),
    )
    .orderBy("amount_count")
)
