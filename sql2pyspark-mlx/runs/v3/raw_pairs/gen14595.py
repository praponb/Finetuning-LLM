from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 19)
    .groupBy(F.hour("txn_time").alias("txn_hour"))
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("avg_amount").desc())
)
