from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.countDistinct("amount").alias("unique_merchants"),
        F.sum(F.when(F.col("merchant") == "7-Eleven", F.col("amount")).otherwise(0)).alias("7_eleven_amount"),
    )
    .filter(F.col("unique_merchants") >= 5)
)
