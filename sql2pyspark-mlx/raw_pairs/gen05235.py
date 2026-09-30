from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status", "order_id")
    .agg(
        F.countDistinct("amount").alias("unique_orders"),
        F.max("paid_at").alias("latest_paid_at"),
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
    .orderBy("latest_paid_at")
)
