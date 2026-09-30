from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2025-05-01")
    .groupBy(
        F.when(F.col("txn_type").isin(["FEE", "REFUND"]), "core").otherwise("other").alias("txn_type_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)
