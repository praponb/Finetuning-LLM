from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"]) & F.col("order_id").isNotNull())
    .groupBy("method")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
    )
    .filter(F.col("failed_count") > 1)
    .drop("failed_count")
    .orderBy("method_values", "method")
    .limit(3)
)
