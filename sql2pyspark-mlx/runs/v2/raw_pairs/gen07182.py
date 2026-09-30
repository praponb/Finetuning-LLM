from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([21, 35]))
    .groupBy("merchant")
    .agg(
        F.countDistinct("amount").alias("unique_merchants"),
        F.sum(F.when(F.col("merchant") == "7-Eleven", F.col("amount")).otherwise(0)).alias("7_eleven_amount"),
    )
    .orderBy(F.col("unique_merchants").desc())
)
