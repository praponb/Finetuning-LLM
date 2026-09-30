from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Grab", "Lazada", "Amazon"]))
    .groupBy("merchant")
    .agg(
        F.max("amount").alias("max_amount"),
        F.min("txn_time").alias("min_txn_time"),
    )
    .filter(F.col("max_amount") > 1300)
)
