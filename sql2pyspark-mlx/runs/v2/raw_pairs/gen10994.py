from pyspark.sql import functions as F

store_summary = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.sum(F.when(F.col("channel") == "store", F.col("shipping_fee")).otherwise(0)).alias("store_shipping_fee"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.store_shipping_fee") > 29)
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("ss.channel_values"),
        F.col("ss.store_shipping_fee"),
    )
    .orderBy(F.col("id").desc())
)
