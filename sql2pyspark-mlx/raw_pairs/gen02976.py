from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy(F.col("order_item_unit_price").desc_nulls_first())

cleaned = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-07-13").cast("date"))
    .select(
        "*",
        (F.col("stock_qty") - F.col("cost")).alias("stock_qty_minus_cost"),
        F.date_sub("launch_date", 51).alias("launch_date_minus_51d"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_filled"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)

latest = (
    with_last
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "price",
        "cost",
        "launch_date",
        "brand_filled",
        "order_item_unit_price",
    )
)

kept_keys = (
    latest
    .select("id")
    .subtract(
        df_products
        .filter(F.col("launch_date") >= "2025-04-01")
        .select("id")
    )
)

verified = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "price",
        "cost",
        "launch_date",
        "brand_filled",
        "order_item_unit_price",
    )
    .orderBy(F.col("cost").desc_nulls_last())
)
