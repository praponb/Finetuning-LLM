from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([22, 24, 26]))
    .groupBy("is_flagged", "merchant")
    .agg(
        F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0)).alias("7_eleven_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .filter(F.col("amount_range") > 30)
)
