from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["DEBIT", "CREDIT", "REFUND"]) & F.col("amount").between(1100, 2100))
    .groupBy("is_flagged", F.hour("txn_time").alias("txn_hour"))
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.max("amount").alias("max_amount"),
    )
)
