from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy("is_flagged", F.month("txn_time").alias("txn_month"))
    .agg(
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("amount")).otherwise(0)).alias("refund_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
        F.min("txn_time").alias("first_txn_time"),
    )
    .filter(F.col("refund_amount") > 25)
)
