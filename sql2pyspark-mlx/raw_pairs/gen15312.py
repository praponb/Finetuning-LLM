from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("store_id").isin([5, 6]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
    )
)

joined = (
    cleaned
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .filter(F.col("b.city") != "Chiang Mai")
    .select("s.*", F.col("b.sqft"))
)

with_dims = (
    joined
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.sale_date",
        "src.revenue",
        "src.revenue_per_unit",
        F.col("s.city"),
        F.col("s.store_name"),
    )
)

joined_store_sales = (
    with_dims
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "inner")
    .select("e.*", F.col("s.region"))
)

valid = (
    joined_store_sales
    .filter((F.col("revenue_per_unit") >= 75) & (F.col("sale_date") >= F.to_date(F.lit("2023-04-01"))))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        "revenue_per_unit",
        "store_name",
        "region",
        F.substring("store_name", 1, 4).alias("store_name_prefix"),
    )
)

result = (
    valid
    .select("id", "store_id", "product_id", "revenue", "region", "store_name_prefix")
    .orderBy(F.col("product_id").desc())
)
