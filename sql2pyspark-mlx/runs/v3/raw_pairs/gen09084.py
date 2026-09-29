from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") & F.col("txn_type").isin(["CREDIT", "DEBIT"]) & F.col("merchant").isin(["Amazon", "7-Eleven", "Grab"]))
    .groupBy("txn_type", "merchant")
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") > 5)
)
