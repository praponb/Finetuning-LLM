from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "*",
        F.sort_array("tags").alias("sorted_tags"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_filled"),
    )
)

result = (
    filtered
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "left")
    .select(
        "src.id",
        "src.product_name",
        "src.brand",
        "src.cost",
        "src.tags",
        "src.supplier_id",
        "src.sorted_tags",
        F.col("s.supplier_name"),
        F.col("s.city"),
    )
)
