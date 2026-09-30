from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "status").orderBy("amount", "id")

cleaned = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-08-04").cast("date")) & (F.col("discount") > 0.27))
    .select("*", (F.col("amount") + F.col("discount")).alias("amount_plus_discount"))
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "order_date", "status", "channel", "amount", "shipping_fee")
)

with_window = (
    ranked
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & F.col("src.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

not_flagged = (
    with_window
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") >= F.col("s.order_date")), "left_anti")
)

not_flagged_orders = (
    not_flagged
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.unit_price") <= 130), "left_semi")
)

result = (
    not_flagged_orders
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        "promo_id",
        "promo_code",
        "discount_pct",
    )
    .orderBy(F.col("shipping_fee").asc_nulls_first(), "status")
)
