from pyspark.sql import functions as F

result = (
    df_transactions
    .withColumn("months_since_txn_time", (F.datediff(F.current_date(), "txn_time") / 30))
    .withColumn("txn_type_prefix", F.substring("txn_type", 1, 2))
    .orderBy("txn_time")
)
