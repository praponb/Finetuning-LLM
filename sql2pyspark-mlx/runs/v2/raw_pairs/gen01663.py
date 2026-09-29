from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") >= 3900)
    .groupBy("is_flagged")
    .agg(
        F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0)).alias("7_eleven_count"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
    )
    .filter(F.col("7_eleven_count") > 31)
    .orderBy(F.col("fee_count").desc())
)
