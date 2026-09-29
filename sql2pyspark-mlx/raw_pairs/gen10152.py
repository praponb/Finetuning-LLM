from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w3 = Window.orderBy(F.col("order_item_unit_price").asc_nulls_last())
w4 = Window.partitionBy("region").orderBy("order_item_unit_price", "id")
w5 = Window.partitionBy("region")
w6 = Window.orderBy(F.col("shipping_fee_filled").desc(), "id")
w7 = Window.partitionBy("channel").orderBy(F.col("order_item_unit_price_bucket").desc(), "id")
w8 = Window.orderBy(F.col("shipping_fee").desc(), "id")

filtered = (
    df_orders
    .filter(F.col("store_id").isNotNull() & (F.col("order_date") > F.lit("2023-06-27").cast("date")))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.quarter("order_date").alias("order_date_quarter"),
        F.round((F.col("discount") * 0.85), 2).alias("discount_scaled"),
    )
)

enriched = (
    filtered
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.store_id",
        "a.channel",
        "a.discount",
        "a.shipping_fee",
        F.col("d.region"),
    )
)

valid = (
    enriched
    .filter(F.col("store_id").isNotNull() & (F.col("channel") != "partner"))
    .select("*", F.coalesce("shipping_fee", F.lit(0)).alias("shipping_fee_filled"))
)

with_latest = (
    valid
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.channel",
        "s.discount",
        "s.shipping_fee",
        "s.region",
        "s.shipping_fee_filled",
        F.col("c.quantity"),
    )
)

with_latest_orders = (
    with_latest
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.channel",
        "s.discount",
        "s.shipping_fee",
        "s.region",
        "s.shipping_fee_filled",
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

with_windows = (
    with_latest_orders
    .select(
        "id",
        "channel",
        "discount",
        "shipping_fee",
        "region",
        "shipping_fee_filled",
        "order_item_unit_price",
        F.dense_rank().over(w3).alias("order_item_unit_price_dense_rank"),
        F.ntile(4).over(w4).alias("order_item_unit_price_bucket"),
        F.stddev_samp("discount").over(w5).alias("region_stddev_samp_discount"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.row_number().over(w6).alias("event_seq"),
        F.ntile(10).over(w7).alias("order_item_unit_price_bucket_bucket"),
        F.ntile(4).over(w8).alias("shipping_fee_bucket"),
    )
)

result = (
    scored
    .groupBy(
        "channel",
        "region",
        F.when(F.col("order_item_unit_price") >= 440, "LARGE").otherwise("SMALL").alias("order_item_unit_price_tier"),
    )
    .agg(
        F.countDistinct("region").alias("distinct_regions"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.size(F.collect_set("region")).alias("num_regions"),
    )
    .orderBy("region")
)
