from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["FEE", "REFUND"]) & F.col("account_id").isin([11, 16, 20, 22]) & (F.to_date("txn_time") >= "2024-06-01"))
    .select("id", "txn_time", F.hour("txn_time").alias("txn_time_hour"))
    .orderBy(F.col("txn_time_hour").desc())
)
