from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)
