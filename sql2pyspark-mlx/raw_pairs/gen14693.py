from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("shipping_fee", "id")
w2 = Window.partitionBy("region").orderBy(F.col("shipping_fee_whole").desc())

filtered = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & F.col("channel").isin(["mobile", "web", "partner"]))
    .select(
        "id",
        "customer_id",
        "store_id",
        "channel",
        "amount",
        "shipping_fee",
        F.upper("channel").alias("channel_upper"),
    )
)

valid = (
    filtered
    .filter(~F.col("channel").eqNullSafe(F.lit("partner")))
    .withColumn("channel_group", F.when(F.col("channel").isin(["mobile", "partner"]), "Tier 1").otherwise("Other"))
    .withColumn("shipping_fee_whole", F.floor("shipping_fee"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("NONE")))
)

joined = (
    valid
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.channel",
        "s.shipping_fee",
        "s.channel_upper",
        "s.shipping_fee_whole",
        F.col("p2.opened_date"),
        F.col("p2.region"),
    )
)

scored = (
    joined
    .filter(F.col("store_id").isin([20, 29, 36, 37]))
    .withColumn("prev_shipping_fee", F.lag("shipping_fee", 1).over(w))
    .withColumn("shipping_fee_whole_percent_rank", F.percent_rank().over(w2))
)

eligible = (
    scored
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.unit_price") <= 290), "left_anti")
)

result = (
    eligible
    .withColumn("year_start", F.date_trunc("YEAR", "opened_date"))
    .withColumn("shipping_fee_whole_percent_rank_tier", F.when(F.col("shipping_fee_whole_percent_rank") >= 0.62, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["region", "year_start", "shipping_fee_whole_percent_rank_tier"], ["region", "year_start"], ["region"], []],
        "region",
        "year_start",
        "shipping_fee_whole_percent_rank_tier",
    )
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.avg("prev_shipping_fee").alias("avg_prev_shipping_fee"),
        F.grouping_id().alias("grouping_level"),
    )
)
