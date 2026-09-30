from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter(F.col("account_id").isin([3, 17]) & F.col("txn_type").isin(["REFUND", "FEE"]) & F.col("is_flagged"))
)

txn_type_summary = (
    filtered_transactions
    .groupBy("txn_type")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_count"),
    )
)

result = (
    txn_type_summary
    .filter(F.col("starbucks_count") < 4)
    .orderBy(F.col("avg_amount").desc())
)
