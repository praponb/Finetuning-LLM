from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)
