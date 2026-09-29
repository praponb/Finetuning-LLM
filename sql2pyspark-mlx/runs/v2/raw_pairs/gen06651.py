from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.max("amount").alias("max_amount"),
    )
)
