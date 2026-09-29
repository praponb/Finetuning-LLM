from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter(F.hour("txn_time").between(12, 18) & F.col("account_id").isin([20, 23, 27, 32]))
)

txn_type_summary = (
    recent_transactions
    .groupBy("txn_type")
    .agg(
        F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0)).alias("7_eleven_count"),
    )
)

result = txn_type_summary.orderBy(F.col("txn_type").desc())
