from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
    .orderBy("merchant_values")
)
