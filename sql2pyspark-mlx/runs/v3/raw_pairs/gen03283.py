from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("order_date") < "2024-05-11") & ~F.col("channel").isin(["mobile", "store"]))
    .groupBy("status", "channel")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.min("shipping_fee").alias("min_shipping_fee"),
    )
    .filter(F.col("min_shipping_fee") >= 24)
    .drop("min_shipping_fee")
    .orderBy(F.col("channel").desc(), "status")
    .limit(3)
)
