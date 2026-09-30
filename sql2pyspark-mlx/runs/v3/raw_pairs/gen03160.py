from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") & (F.col("merchant") == "Starbucks"))
    .groupBy("merchant", "txn_type", F.to_date("txn_time").alias("txn_day"))
    .agg(F.min("amount").alias("min_amount"))
    .orderBy("txn_day", F.col("merchant").desc())
)
