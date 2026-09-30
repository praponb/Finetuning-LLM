from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

valid = (
    df_orders
    .filter(F.col("store_id").isNotNull() & (F.year("order_date") >= 2024))
    .select(
        "*",
        (F.col("discount") - F.col("amount")).alias("discount_minus_amount"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

with_latest = (
    valid
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)

remaining_keys = (
    with_latest
    .select("store_id", "status")
    .intersectAll(
        df_orders
        .filter(F.col("discount") >= 0.16)
        .select("store_id", "status")
    )
)

kept_rows = (
    with_latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

attributed = (
    kept_rows
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & F.col("s.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    attributed
    .select(
        "id",
        "status",
        "discount",
        "shipping_fee",
        "discount_minus_amount",
        "channel_filled",
        "order_item_unit_price",
        "promo_id",
        "promo_code",
    )
)
