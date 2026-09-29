from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.hour("txn_time").alias("txn_hour"))
    .pivot("txn_type", ["FEE", "REFUND", "CREDIT"])
    .agg(F.sum("amount"))
    .orderBy("txn_hour")
)
