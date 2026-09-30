from pyspark.sql import functions as F

recent_transactions = df_transactions.filter(F.col("is_flagged") & (F.col("amount") >= 1500))

agg = (
    recent_transactions
    .groupBy("merchant", "is_flagged")
    .agg(
        F.sum(F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0)).alias("debit_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = agg.select("merchant", "debit_count", "avg_amount")
