from pyspark.sql import functions as F

filtered_payments = df_payments.filter(F.col("order_id").isNull())

method_summary = (
    filtered_payments
    .groupBy("method")
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
)

result = (
    method_summary
    .orderBy("method_values", "first_paid_at", "method")
    .limit(10)
)
