from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter((~F.col("txn_type").isin(["FEE", "REFUND", "DEBIT"]) & (F.col("merchant") == "Shell")) | F.col("account_id").isin([9, 13, 15]))
)

transactions_stats = (
    filtered_transactions
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("credit_amount"),
    )
)

result = transactions_stats
