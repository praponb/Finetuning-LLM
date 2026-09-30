from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & F.col("account_id").isin([19, 36]) & (F.col("merchant") == "Lazada"))
    .select("amount", "txn_time", F.dayofmonth("txn_time").alias("txn_time_day"))
    .orderBy("txn_time_day")
)
