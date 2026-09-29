from pyspark.sql import functions as F

src = (
    df_transactions
    .filter(F.col("account_id").isin([1, 14, 21, 22]) & (~F.col("txn_type").isin(["FEE", "CREDIT"]) | (F.col("amount") < 3200)))
)

txn_hour_summary = (
    src
    .groupBy(F.hour("txn_time").alias("txn_hour"))
    .agg(F.count("*").alias("total_count"))
)

result = txn_hour_summary.filter(F.col("total_count") < 4)
