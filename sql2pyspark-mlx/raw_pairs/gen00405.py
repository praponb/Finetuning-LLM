from pyspark.sql import functions as F

valid_transactions = df_transactions.filter(F.col("txn_type").isin(["REFUND", "FEE", "DEBIT"]))

is_flagged_summary = (
    valid_transactions
    .groupBy("is_flagged")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.min("amount").alias("min_amount"),
    )
)

result = is_flagged_summary.select("is_flagged", "total_amount", "min_amount")
