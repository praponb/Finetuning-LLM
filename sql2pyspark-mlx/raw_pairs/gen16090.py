from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "7-Eleven")
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
        F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0)).alias("refund_count"),
    )
    .filter(F.col("refund_count") < 70)
    .orderBy("txn_type_values")
)
