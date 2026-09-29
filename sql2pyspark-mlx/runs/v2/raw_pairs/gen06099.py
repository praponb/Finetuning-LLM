from pyspark.sql import functions as F

result = (
    df_transactions
    .cube(F.quarter("txn_time").alias("txn_quarter"), "txn_type")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.max("txn_time").alias("last_txn_time"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
    .orderBy("txn_quarter")
)
