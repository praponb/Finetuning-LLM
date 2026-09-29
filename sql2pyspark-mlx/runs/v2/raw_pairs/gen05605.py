from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "CREDIT")
    .groupBy("account_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fee"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
    .filter(F.col("pct_fee") >= 46)
    .select("account_id", "pct_fee", "txn_type_values")
)
