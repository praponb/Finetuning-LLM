from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.hour("txn_time").alias("txn_hour"))
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.count("*").alias("num_transactions"),
    )
    .orderBy("amount_range")
)
