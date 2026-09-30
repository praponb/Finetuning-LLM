from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") < 1500) & (F.col("amount") >= 2200) & F.col("account_id").isin([3, 17, 33]))
    .groupBy("is_flagged", F.hour("txn_time").alias("txn_hour"))
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.max("txn_time").alias("latest_txn_time"),
    )
)
