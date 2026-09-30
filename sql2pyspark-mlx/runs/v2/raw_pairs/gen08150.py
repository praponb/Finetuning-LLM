from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(F.col("merchant") == "Amazon")
    .withColumn("txn_time_period", F.date_trunc("year", "txn_time"))
)

is_flagged_summary = (
    valid_transactions
    .groupBy("is_flagged")
    .agg(F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"))
)

result = (
    is_flagged_summary
    .filter(F.col("credit_count") < 8)
    .select("is_flagged", "credit_count")
)
