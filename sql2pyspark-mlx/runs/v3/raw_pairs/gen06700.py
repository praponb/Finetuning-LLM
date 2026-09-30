from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") != "Starbucks") & ((F.col("txn_time") >= "2023-03-01") | F.col("merchant").isNotNull()))
    .groupBy("txn_type")
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("avg_amount") >= 3100)
    .orderBy(F.col("first_txn_time").desc())
)
