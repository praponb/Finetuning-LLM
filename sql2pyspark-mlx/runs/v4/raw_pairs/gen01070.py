from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("debit_amount") >= 72)
    .orderBy("amount_count", F.col("txn_quarter").desc())
)
