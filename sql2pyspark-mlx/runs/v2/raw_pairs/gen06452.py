from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "FEE")
    .groupBy("txn_type")
    .agg(
        F.count("*").alias("transaction_count"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") >= 2400)
)
