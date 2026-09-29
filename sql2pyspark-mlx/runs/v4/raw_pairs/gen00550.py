from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([8, 35, 36]) & (F.col("txn_type") != "DEBIT"))
    .groupBy(F.quarter("txn_time").alias("txn_quarter"), "merchant")
    .agg(F.count("amount").alias("amount_count"))
    .orderBy("merchant")
)
