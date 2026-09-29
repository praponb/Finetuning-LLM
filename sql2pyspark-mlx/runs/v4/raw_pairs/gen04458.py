from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipping_fee")
w2 = Window.partitionBy("channel").orderBy(F.col("unit_price").desc_nulls_last())
w3 = Window.partitionBy("product_id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.round("unit_price", 0).alias("unit_price_rounded"),
    )
)

matched_keys = (
    filtered
    .select("id", "product_id")
    .intersect(
        df_order_items
        .filter(F.col("order_id").isin([6, 14, 27, 34]))
        .select("id", "product_id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.channel"), F.col("d.order_date"), F.col("d.shipping_fee"))
)

result = (
    with_dims
    .select(
        "*",
        F.rank().over(w).alias("shipping_fee_rank"),
        F.rank().over(w2).alias("unit_price_rank"),
        F.count("shipping_fee").over(w3).alias("product_id_count_shipping_fee"),
    )
)
