from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter((F.col("txn_time") >= "2024-11-05") & ((F.col("txn_type") == "DEBIT") | F.col("txn_type").isin(["FEE", "CREDIT", "REFUND"])))
)

transactions_stats = (
    recent_transactions
    .groupBy("txn_type", "merchant")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    transactions_stats
    .filter(F.col("pct_refund") >= 43)
    .orderBy(F.col("txn_type").desc())
)
