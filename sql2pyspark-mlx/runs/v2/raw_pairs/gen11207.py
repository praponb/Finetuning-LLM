from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2024-04-15")
    .groupBy("method", "status", "order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .orderBy("first_paid_at", "method", "status", "order_id")
    .limit(1)
)
