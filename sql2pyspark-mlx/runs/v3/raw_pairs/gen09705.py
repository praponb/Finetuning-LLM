from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.to_date("txn_time").alias("txn_day"))
    .pivot("txn_type", ["DEBIT", "FEE", "REFUND"])
    .agg(F.count("amount"))
)
