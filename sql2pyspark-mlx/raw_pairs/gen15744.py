from pyspark.sql import functions as F

joined = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.shipped_date",
        "a.delivered_date",
        "a.cost",
        "a.status",
        F.col("d.shipping_fee"),
        F.col("d.discount"),
        F.col("d.channel"),
    )
)

result = (
    joined
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "status",
        "shipping_fee",
        "discount",
        "channel",
        F.when(F.col("channel").rlike("^(partner|store)$"), "OTHER_ZONE").when(F.col("channel").rlike("^(web|mobile)$"), "GROUP_A").otherwise("PRIORITY").alias("channel_zone"),
    )
    .orderBy(F.col("shipped_date").asc_nulls_first(), "channel_zone")
)
