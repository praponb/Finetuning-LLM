from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("quantity", "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.when(F.col("quantity") >= 8, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low").alias("quantity_level"),
        F.when(F.col("unit_price") >= 310, "High").when(F.col("unit_price") >= 60, "Medium").otherwise("Low").alias("unit_price_level"),
    )
)

joined = (
    cleaned
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.shipping_fee") > 34)
    .select("e.*", F.col("b.discount"), F.col("b.channel"))
)

with_dims = (
    joined
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id").eqNullSafe(F.col("d.id")), "left")
    .select("e.*", F.col("d.category"), F.col("d.brand"))
)

latest = (
    with_dims
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount",
        "channel",
        "category",
        "rn",
        F.when(F.col("channel").rlike("^(partner|mobile)$"), "EXTENDED").when(F.col("channel").rlike("^(web|store)$"), "GROUP_B").otherwise("GROUP_A").alias("channel_zone"),
    )
    .orderBy(F.col("product_id").desc())
)
