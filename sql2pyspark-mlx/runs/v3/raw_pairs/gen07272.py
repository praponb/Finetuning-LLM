from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "SHIPPED")
    .groupBy("channel")
    .agg(
        F.max("discount").alias("max_discount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
    )
)
