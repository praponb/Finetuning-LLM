from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "REFUND")
    .select("is_flagged", F.date_trunc("YEAR", "txn_time").alias("txn_time_period"))
)
