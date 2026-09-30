from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(F.col("merchant").isin(["Lazada", "Grab", "Starbucks"]) | F.col("account_id").isin([1, 32, 34]))
)

summary = (
    valid_transactions
    .groupBy(
        F.quarter("txn_time").alias("txn_quarter"),
        F.date_format("txn_time", "yyyy-MM").alias("txn_month"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
    )
)

result = summary.orderBy("fee_count", "avg_amount")
