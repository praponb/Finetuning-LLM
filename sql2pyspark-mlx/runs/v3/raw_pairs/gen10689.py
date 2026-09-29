from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter(F.col("merchant").isin(["Shell", "Grab", "Starbucks"]) & F.col("merchant").isin(["Lazada", "Shell", "Grab"]) & F.col("account_id").isin([3, 40]))
)

is_flagged_summary = (
    recent_transactions
    .groupBy("is_flagged")
    .agg(F.countDistinct("amount").alias("unique_txn_types"))
)

result = (
    is_flagged_summary
    .orderBy("unique_txn_types", "is_flagged")
    .limit(10)
)
