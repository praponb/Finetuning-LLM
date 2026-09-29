from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.hour("txn_time").alias("txn_hour"),
        "txn_type",
        F.date_trunc("MONTH", "txn_time").alias("txn_month"),
    )
    .agg(F.sort_array(F.collect_set("merchant")).alias("merchant_list"))
    .orderBy(F.col("txn_month").desc(), F.col("txn_hour").desc(), "txn_type")
    .limit(20)
)
