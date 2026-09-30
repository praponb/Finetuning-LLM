from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(
        F.countDistinct("amount").alias("unique_txn_types"),
        F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0)).alias("7_eleven_count"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy("7_eleven_count")
)
