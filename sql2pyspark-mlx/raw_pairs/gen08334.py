from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 21) | F.col("order_id").isNotNull())
    .groupBy("status", "order_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.min("paid_at").alias("first_paid_at"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy("status")
)
