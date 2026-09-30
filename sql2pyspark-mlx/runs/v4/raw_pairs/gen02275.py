from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("order_id")
w_grp = Window.partitionBy("order_id").orderBy("unit_price", "id")
w3 = Window.partitionBy("order_channel")
w4 = Window.partitionBy("channel").orderBy(F.col("shipping_fee").desc_nulls_first())

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("product_id").isin([10, 11, 16]))
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)

scored = (
    base
    .select(
        "*",
        F.max("unit_price").over(w_recent).alias("order_id_max_unit_price"),
        F.lead("quantity", 2).over(w_grp).alias("next_quantity"),
    )
)

joined = (
    scored
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.shipping_fee"), F.col("p.channel"))
)

with_dims = (
    joined
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.channel").alias("order_channel"), F.col("b.amount"))
)

enriched = (
    with_dims
    .select(
        "*",
        F.count("amount").over(w3).alias("order_channel_count_amount"),
        F.percent_rank().over(w4).alias("shipping_fee_percent_rank"),
    )
)

joined_order_items = (
    enriched
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.amount").alias("order_amount"), F.col("p.status"))
)

result = (
    joined_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "order_id_max_unit_price",
        "next_quantity",
        "shipping_fee",
        "channel",
        "order_channel",
    )
)
