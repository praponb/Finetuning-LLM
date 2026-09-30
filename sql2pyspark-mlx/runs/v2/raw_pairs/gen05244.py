from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .rollup("is_flagged", "txn_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
)
