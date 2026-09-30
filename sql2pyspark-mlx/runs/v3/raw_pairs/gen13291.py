from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Starbucks") | F.col("merchant").isin(["7-Eleven", "Amazon"]))
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(F.min("amount").alias("min_amount"))
    .orderBy(F.col("min_amount").desc())
)
