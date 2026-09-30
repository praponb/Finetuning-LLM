from pyspark.sql import functions as F

src = (
    df_orders
    .filter(F.col("discount").isNotNull() & (F.col("store_id").isin([13, 39, 40]) | F.col("store_id").isin([1, 10, 30, 34])))
)

orders_stats = (
    src
    .groupBy("status")
    .agg(
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
)

result = orders_stats.select("status", "shipping_fee_range", "channel_values")
