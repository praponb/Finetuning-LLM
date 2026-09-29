from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("channel").orderBy(F.col("discount").asc_nulls_first(), "id")

filtered = (
    df_orders
    .filter(F.col("store_id").isNotNull())
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
        F.coalesce("channel", F.lit("Unknown")).alias("channel_filled"),
        F.when(F.col("discount") >= 0.33, "High").when(F.col("discount") >= 0.15, "Medium").otherwise("Low").alias("discount_band"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.delivered_date").alias("shipment_delivered_date"),
        F.col("c.carrier").alias("shipment_carrier"),
    )
)

joined = (
    with_latest
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.sqft"))
)

deduped = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "discount_band",
        "shipment_delivered_date",
        F.when(F.col("channel").rlike("^(mobile|web)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("channel_zone"),
    )
)
