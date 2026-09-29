from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([7, 21, 30]) & (F.col("paid_at") >= "2025-10-30"))
    .groupBy("method", "order_id", "status")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
    .orderBy(F.col("order_id").desc())
)
