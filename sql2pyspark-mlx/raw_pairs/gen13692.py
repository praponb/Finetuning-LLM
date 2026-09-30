from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").asc_nulls_first(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("adjusted_discount").desc_nulls_first(), "id")

base = (
    df_orders
    .filter((F.col("discount") > 0.18) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "store_id",
        "amount",
        "discount",
        "shipping_fee",
        F.concat("channel", F.lit("/"), "status").alias("channel_status"),
        F.date_trunc("week", "order_date").alias("order_date_period"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.delivered_date") <= F.col("s.order_date_period")), "left_anti")
)

valid = (
    eligible
    .filter(F.col("discount") <= 0.4)
    .select(
        "id",
        "store_id",
        "discount",
        "shipping_fee",
        "channel_status",
        "channel_filled",
        F.round((F.col("discount") * 0.85), 2).alias("adjusted_discount"),
        F.when(F.col("discount") >= 0.39, "High").when(F.col("discount") >= 0.14, "Medium").otherwise("Low").alias("discount_tier"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "store_id",
        "shipping_fee",
        "channel_status",
        "channel_filled",
        "adjusted_discount",
    )
)

deduped = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city").alias("city"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("a.shipping_fee")), 2).alias("avg_shipping_fee"),
    )
    .orderBy("avg_shipping_fee")
)
