from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(10, 14))
    .groupBy(F.hour("txn_time").alias("txn_hour"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
        F.min("txn_time").alias("first_txn_time"),
    )
    .filter(F.col("pct_credit") >= 60)
)
