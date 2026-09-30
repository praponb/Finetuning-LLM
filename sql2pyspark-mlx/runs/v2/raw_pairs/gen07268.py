from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter((F.col("txn_type") == "REFUND") & (F.col("account_id") == 36) & (F.col("txn_type") == "REFUND"))
)

txn_month_summary = (
    valid_transactions
    .groupBy(F.month("txn_time").alias("txn_month"), "txn_type")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count("*").alias("cnt"),
    )
)

result = txn_month_summary.select("txn_month", "txn_type", "avg_amount", "cnt")
