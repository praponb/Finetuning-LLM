from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged", "txn_type")
    .agg(
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("amount")).otherwise(0)).alias("fee_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
    .orderBy(F.col("txn_type").desc())
)
