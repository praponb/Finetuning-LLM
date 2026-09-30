from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(
        F.sum(F.when(F.col("merchant") == "7-Eleven", F.col("amount")).otherwise(0)).alias("7_eleven_amount"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
    .orderBy(F.col("7_eleven_amount").desc())
)
