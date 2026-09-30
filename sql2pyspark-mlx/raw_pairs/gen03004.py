from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"),
        F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0)).alias("7_eleven_count"),
    )
    .filter(F.col("debit_count") >= 7)
    .orderBy(F.col("account_id").desc())
)
