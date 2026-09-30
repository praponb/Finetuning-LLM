from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category")
w_ordered = Window.partitionBy("brand", "category")

cleaned = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "launch_date",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)

remaining_keys = (
    latest_match
    .select("brand", "id")
    .subtract(
        df_products
        .filter(F.length("product_name") >= 13)
        .select("brand", "id")
    )
)

reconciled = (
    latest_match
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "*",
        F.round((F.col("price") / F.sum("price").over(w2)), 4).alias("price_share"),
        F.max("order_item_unit_price").over(w_ordered).alias("brand_max_order_item_unit_price"),
    )
)

result = (
    enriched
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "launch_date",
        "cost_filled",
        "order_item_unit_price",
        "brand_max_order_item_unit_price",
    )
    .orderBy(F.col("brand_max_order_item_unit_price").desc())
)
