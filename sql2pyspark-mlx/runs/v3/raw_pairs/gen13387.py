from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
    )
    .filter(F.col("refund_count") < 5)
    .select("is_flagged", "merchant_list", "refund_count")
    .orderBy("refund_count")
)
