from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") != "Amazon") & (F.col("amount") <= 2900))
    .groupBy(F.month("txn_time").alias("txn_month"), "txn_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
    .orderBy(F.col("txn_month").desc())
)
