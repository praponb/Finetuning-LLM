from pyspark.sql import functions as F

with_dims = (
    df_order_items
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.shipping_fee").isNotNull())
    .select("s.*", F.col("b.channel"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("unit_price") >= 320, "LARGE").otherwise("SMALL").alias("unit_price_tier"),
    )
    .agg(
        F.count_if(F.col("channel") == "web").alias("web_events"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.size(F.collect_set("channel")).alias("num_channels_2"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .orderBy(F.col("web_events").desc_nulls_first())
)
