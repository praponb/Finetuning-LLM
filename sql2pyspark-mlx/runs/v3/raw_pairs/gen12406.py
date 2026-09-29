from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter((F.col("txn_time") >= "2023-11-12") & F.col("txn_type").isin(["DEBIT", "REFUND", "CREDIT"]))
)

txn_month_summary = (
    valid_transactions
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(
        F.max("amount").alias("max_amount"),
        F.count("*").alias("total_count"),
    )
)

result = (
    txn_month_summary
    .filter(F.col("max_amount") >= 2000)
    .select("txn_month", "total_count")
)
