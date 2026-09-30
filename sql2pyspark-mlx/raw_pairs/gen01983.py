from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["CREDIT", "REFUND", "FEE"]))
    .withColumn("txn_time_hour", F.hour("txn_time"))
    .withColumn("weeks_since_txn_time", (F.datediff(F.current_date(), "txn_time") / 7))
    .orderBy("weeks_since_txn_time")
)
