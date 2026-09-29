from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "DEBIT")
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg(
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
        F.count("*").alias("transaction_count"),
    )
)
