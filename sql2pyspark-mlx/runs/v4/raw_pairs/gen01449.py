from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(((F.col("merchant") == "Shell") & (F.col("txn_time") < "2024-08-10")) | F.col("merchant").isNotNull())
    .groupBy("account_id", F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(F.max("amount").alias("max_amount"))
)
