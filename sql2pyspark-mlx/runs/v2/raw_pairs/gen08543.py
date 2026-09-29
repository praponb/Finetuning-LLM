from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter((F.to_date("txn_time") < "2025-07-01") & ~F.col("is_flagged"))
)

summary = (
    filtered_transactions
    .groupBy("txn_type", "is_flagged")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    summary
    .filter(F.col("avg_amount") >= 27)
    .select("txn_type", "is_flagged", "avg_amount")
    .orderBy("txn_type", "is_flagged")
    .limit(50)
)
