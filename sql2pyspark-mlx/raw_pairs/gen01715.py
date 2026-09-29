from pyspark.sql import functions as F

src = df_transactions.filter(F.col("is_flagged"))

transactions_stats = (
    src
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0)).alias("7_eleven_count"),
        F.sum(F.when(F.col("merchant") == "7-Eleven", F.col("amount")).otherwise(0)).alias("7_eleven_amount"),
    )
)

result = transactions_stats
