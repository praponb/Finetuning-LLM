from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

joined = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.country"), F.col("d.city"), F.col("d.rating"))
)

cleaned = (
    joined
    .filter(F.col("product_name").like("wir%"))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "stock_qty",
        "tags",
        "supplier_id",
        F.size("tags").alias("tags_count"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.quantity") >= 131), "left_semi")
)

with_last = (
    eligible
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.last_restock"))
)

joined_products = (
    with_last
    .alias("s")
    .join(df_suppliers.alias("b"), F.col("s.supplier_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.country"), F.col("b.supplier_name"))
)

cleaned_products = (
    joined_products
    .filter(F.col("supplier_id").isNotNull())
    .withColumn("country_title", F.initcap(F.trim("country")))
)

result = (
    cleaned_products
    .groupBy("brand")
    .pivot("category", ["Fashion", "Home"])
    .agg(F.max("stock_qty"))
)
