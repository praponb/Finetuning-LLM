from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        "is_flagged",
        "txn_type",
        F.date_format("txn_time", "yyyy-MM").alias("txn_month"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
    .orderBy("txn_type", "is_flagged", "txn_month")
    .limit(5)
)
