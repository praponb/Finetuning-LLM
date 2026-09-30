from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") | F.col("account_id").isin([7, 18, 19, 22]))
    .groupBy(
        "txn_type",
        F.month("txn_time").alias("txn_month"),
        F.year("txn_time").alias("txn_year"),
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0)).alias("debit_count"),
    )
)
