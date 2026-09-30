from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .groupBy(
        F.col("o.channel"),
        F.when(F.col("o.shipping_fee") >= 36, "high").when(F.col("o.shipping_fee") >= 15, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.status")))).alias("status_values"),
    )
)
