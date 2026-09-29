from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2025-04-01")
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_failed"),
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
    .filter(F.col("pct_failed") < 76)
    .orderBy(F.col("pct_failed").desc(), "status")
    .limit(5)
)
