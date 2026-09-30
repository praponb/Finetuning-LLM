from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") <= 3400) & (F.col("txn_type") == "REFUND"))
    .agg(
        F.max("amount").alias("max_amount"),
        F.max("txn_time").alias("latest_txn_time"),
    )
    .orderBy(F.col("max_amount").desc())
)
