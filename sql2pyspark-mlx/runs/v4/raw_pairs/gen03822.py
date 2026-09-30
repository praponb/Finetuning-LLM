from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "channel").orderBy("discount", "id")
w2 = Window.partitionBy("channel").orderBy(F.col("shipping_fee_whole").asc_nulls_last())
w3 = Window.partitionBy("channel", "region").orderBy(F.col("amount").asc_nulls_last())
w4 = Window.partitionBy("region")
w5 = Window.partitionBy("channel")

cleaned = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-09-20").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.floor("shipping_fee").alias("shipping_fee_whole"),
        F.ceil("discount").alias("discount_whole"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

cleaned_orders = (
    cleaned
    .filter((F.col("order_date") > F.lit("2024-01-12").cast("date")) & F.col("channel").isNotNull())
    .withColumn("greatest_discount_shipping_fee_whole", F.greatest("discount", "shipping_fee_whole"))
)

joined = (
    cleaned_orders
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.opened_date"), F.col("d.region"))
)

enriched = (
    joined
    .withColumn("discount_bucket", F.ntile(4).over(w))
    .withColumn("shipping_fee_whole_cume_dist", F.cume_dist().over(w2))
)

remaining_keys = (
    enriched
    .select("store_id", "channel")
    .intersect(
        df_orders
        .filter(F.col("channel") != "mobile")
        .select("store_id", "channel")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

cleaned_orders_2 = (
    kept_rows
    .filter(~F.col("region").isin(["North", "East"]))
    .select(
        "id",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "shipping_fee_whole",
        "discount_whole",
        "resolved_channel",
        "greatest_discount_shipping_fee_whole",
        "opened_date",
        "region",
        "discount_bucket",
        "shipping_fee_whole_cume_dist",
        F.date_format("order_date", "yyyy-MM").alias("order_date_label"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel_2"),
    )
)

enriched_orders = (
    cleaned_orders_2
    .withColumn("amount_rank", F.rank().over(w3))
    .withColumn("region_stddev_samp_shipping_fee_whole", F.stddev_samp("shipping_fee_whole").over(w4))
    .withColumn("channel_stddev_samp_amount", F.stddev_samp("amount").over(w5))
)

result = (
    enriched_orders
    .select(
        "channel",
        "discount",
        "shipping_fee",
        "shipping_fee_whole",
        "resolved_channel",
        "greatest_discount_shipping_fee_whole",
        "opened_date",
        "region",
        "order_date_label",
    )
    .orderBy(F.col("channel").desc(), "shipping_fee_whole")
)
