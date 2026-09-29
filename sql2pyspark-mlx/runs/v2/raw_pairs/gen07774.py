from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 1230)
    .groupBy("status", "order_id")
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.round(((100 * F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
    )
    .orderBy(F.col("status").desc(), "order_id")
    .limit(5)
)
