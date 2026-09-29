from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.hour("txn_time").alias("txn_hour"), "merchant")
    .agg(F.count("amount").alias("amount_count"))
    .filter(F.col("amount_count") < 3)
)
