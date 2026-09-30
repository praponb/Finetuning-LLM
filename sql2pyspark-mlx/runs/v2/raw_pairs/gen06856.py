from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("txn_type")
    .agg(F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"))
    .orderBy("txn_type")
    .limit(5)
)
