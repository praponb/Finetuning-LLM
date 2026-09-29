from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("status") == "FAILED") & (F.hour("paid_at").between(7, 15) | (F.col("method") == "cash")))
)

method_summary = (
    base
    .groupBy("method", "order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
    )
)

result = (
    method_summary
    .filter(F.col("success_count") < 7)
    .orderBy(F.col("method").desc(), "success_count")
)
