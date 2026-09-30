from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 4)
    .groupBy("merchant")
    .agg(
        F.countDistinct("amount").alias("unique_merchants"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Lazada", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lazada"),
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_count"),
    )
    .filter(F.col("7_eleven_count") < 5)
    .drop("7_eleven_count")
    .orderBy("unique_merchants")
)
