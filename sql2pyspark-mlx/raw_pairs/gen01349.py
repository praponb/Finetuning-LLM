from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([5, 12, 27, 34]))
    .groupBy(F.year("txn_time").alias("txn_year"))
    .agg(
        F.sum(F.when(F.col("merchant") == "7-Eleven", F.col("amount")).otherwise(0)).alias("7_eleven_amount"),
        F.min("txn_time").alias("first_txn_time"),
    )
    .filter(F.col("7_eleven_amount") >= 79)
)
