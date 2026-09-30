from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "category",
        "price",
        "cost",
        "tags",
        "supplier_id",
        F.coalesce("brand", F.lit("N/A")).alias("brand_filled"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_clean"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.tags", "s.brand_filled", F.col("c.unit_price"))
)

kept = (
    with_latest
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.order_id").isin([1, 7, 10, 14])), "left_anti")
)

filtered_products = (
    kept
    .filter(F.col("unit_price") <= 110)
    .withColumn("tags_count", F.size("tags"))
)

result = (
    filtered_products
    .select("id", "tags", "brand_filled")
    .orderBy(F.col("id").desc_nulls_last())
)
