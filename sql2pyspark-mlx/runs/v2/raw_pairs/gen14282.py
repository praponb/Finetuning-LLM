from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.hour("txn_time").alias("txn_hour"))
    .agg(F.sort_array(F.collect_set("merchant")).alias("merchant_list"))
    .orderBy("txn_hour")
)
