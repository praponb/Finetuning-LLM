from pyspark.sql import functions as F

result = (
    df_transactions
    .rollup(F.quarter("txn_time").alias("txn_quarter"), "is_flagged")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
)
