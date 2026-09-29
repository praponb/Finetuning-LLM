from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter((F.col("status").isin(["REFUNDED", "SUCCESS", "FAILED"]) & (F.col("paid_at") >= "2025-02-28")) | F.col("order_id").isNotNull())
)

status_summary = (
    filtered_payments
    .groupBy("status")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    status_summary
    .filter(F.col("avg_amount") > 28)
    .select("status", "avg_amount", "status_values")
    .orderBy(F.col("status_values").desc(), "status")
    .limit(20)
)
