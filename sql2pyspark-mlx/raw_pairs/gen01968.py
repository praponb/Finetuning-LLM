from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(~F.col("city").eqNullSafe(F.lit("New York")))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

filtered = (
    valid
    .filter(F.col("city") == "Singapore")
    .withColumn("city_title", F.initcap(F.trim("city")))
    .withColumn("city_clean", F.coalesce("city", F.lit("NONE")))
)

with_latest = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        "s.city_clean",
        F.col("c.category").alias("product_category"),
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

matched_keys = (
    with_latest
    .select("id", "country")
    .subtract(
        df_suppliers
        .filter(F.col("country") == "SG")
        .select("id", "country")
    )
)

reconciled = (
    with_latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("city").isNotNull() & F.col("product_category").isin(["Electronics", "Books", "Grocery"]))
    .select(
        "*",
        F.concat("country", F.lit(" - "), "product_category").alias("country_product_category"),
    )
)

result = (
    base
    .select(
        "country",
        "city",
        "city_clean",
        "product_stock_qty",
        "country_product_category",
    )
    .orderBy(F.col("city").desc_nulls_last(), F.col("country").desc())
)
