from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([8, 11, 26, 31]))
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(
        F.sum(F.when(F.col("merchant") == "7-Eleven", F.col("amount")).otherwise(0)).alias("7_eleven_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .orderBy("txn_month", F.col("7_eleven_amount").desc())
    .limit(50)
)
