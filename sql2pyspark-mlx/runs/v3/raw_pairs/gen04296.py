from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.max("paid_at").alias("last_paid_at"),
    )
)
