from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Grab")
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"), "merchant")
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.avg("amount").alias("avg_amount"),
    )
    .orderBy("avg_amount")
)
