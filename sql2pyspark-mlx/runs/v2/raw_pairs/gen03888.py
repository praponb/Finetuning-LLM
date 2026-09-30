from pyspark.sql import functions as F

order_stats = (
    df_payments
    .filter(F.col("order_id").isNull())
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.countDistinct("amount").alias("unique_statuss"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.col("t.status_values"),
        F.col("t.unique_statuss"),
    )
)
