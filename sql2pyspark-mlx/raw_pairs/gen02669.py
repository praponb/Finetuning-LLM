from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc())
w2 = Window.partitionBy("channel", "order_id").orderBy(F.col("unit_price").desc(), "id")
w3 = Window.partitionBy("channel", "order_id").orderBy(F.col("unit_price").asc_nulls_first(), "id")

filtered = (
    df_order_items
    .filter(F.col("unit_price").isNotNull())
    .withColumn("quantity_band", F.when(F.col("quantity") >= 9, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low"))
)

joined = (
    filtered
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id").isNull())
    .select(
        "src.id",
        "src.order_id",
        "src.unit_price",
        "src.quantity_band",
        F.col("o.shipping_fee"),
        F.col("o.discount"),
    )
)

latest = (
    joined
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_dims = (
    latest
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.unit_price",
        "src.quantity_band",
        "src.shipping_fee",
        "src.discount",
        F.col("p.shipping_fee").alias("order_shipping_fee"),
        F.col("p.channel"),
    )
)

deduped = (
    with_dims
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

ranked = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

joined_order_items = (
    ranked
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.discount",
        "src.channel",
        "src.rn",
        "src.rank_in_group",
        F.col("o.discount").alias("order_discount"),
        F.col("o.channel").alias("order_channel"),
    )
)

result = (
    joined_order_items
    .select("id", "rn", "rank_in_group", "order_discount", "order_channel")
)
