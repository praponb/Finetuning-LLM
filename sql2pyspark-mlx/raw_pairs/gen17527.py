from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.promo_id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id"), F.col("s.promo_id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

base = (
    df_orders
    .filter(F.col("order_date") > F.to_date(F.lit("2023-08-12")))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        F.when(F.col("channel").isin(["web", "partner"]), "Group A").otherwise("Other").alias("channel_group"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

attributed = (
    base
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & F.col("e.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

latest_match = (
    attributed
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.carrier").alias("shipment_carrier"),
        F.col("c.delivered_date"),
    )
)

with_dims = (
    latest_match
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .filter(F.col("d.city") == "New York")
    .select(
        "src.id",
        "src.customer_id",
        "src.store_id",
        "src.channel",
        "src.amount",
        "src.shipping_fee",
        "src.channel_group",
        "src.promo_id",
        "src.promo_code",
        "src.discount_pct",
        "src.shipment_carrier",
        F.col("d.opened_date"),
    )
)

latest_match_orders = (
    with_dims
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.channel",
        "s.amount",
        "s.shipping_fee",
        "s.channel_group",
        "s.promo_id",
        "s.discount_pct",
        "s.opened_date",
        F.col("c.paid_at").alias("payment_paid_at"),
    )
)

result = (
    latest_match_orders
    .select(
        "id",
        "customer_id",
        "channel",
        "shipping_fee",
        "channel_group",
        "promo_id",
        "opened_date",
    )
    .orderBy(F.col("shipping_fee").desc_nulls_last(), F.col("opened_date").desc())
)
